"""药物安全信号可复演窗口计算 —— 端到端剧本演示（无需起服务）。

运行：python3 run_signal_demo.py

剧本覆盖：
1. 登记不可变术语归一版本与监测定义（药品组/事件组/阈值）；
2. W1 窗口提交分片作业 -> 四格/PRR/ROR/卡方 -> 信号触发 -> 分析员确认；
3. 迟到随访（跨事件组迁移）与新病例进入 W2：旧统计贡献撤销、新贡献计入；
4. 新信号修订必须重新处置；旧 CONFIRMED 保留；基于过期修订号被拒绝；
5. 同一配置+截止线重复提交只得到同一作业/同一份结果（输出哈希一致）；
6. 全量 vs 增量：增量从基线窗口复制干净来源、只重算脏来源，结果经独立
   重算核验与冻结输入逐行一致；
7. 分片崩溃 -> 从已提交分片恢复续跑，无半成品指标、无重复贡献；
8. 病例归并/拆解的归属血缘与贡献追踪；审计哈希链核验。
"""
from __future__ import annotations

import json
import tempfile
from datetime import datetime

from pv_service.signals.service import SignalApplication, FixedClock

CATALOG = {"concepts": [
    {"code": "IBU", "kind": "DRUG", "name": "布洛芬",
     "terms": ["ibuprofen", "布洛芬", "ibuprofeno"]},
    {"code": "AMX", "kind": "DRUG", "name": "阿莫西林",
     "terms": ["amoxicillin", "阿莫西林"]},
    {"code": "RASH", "kind": "EVENT", "name": "皮疹",
     "terms": ["rash", "皮疹", "sarpullido"]},
    {"code": "NAU", "kind": "EVENT", "name": "恶心",
     "terms": ["nausea", "恶心"]},
]}

SPEC = {
    "drug_groups": [
        {"id": "D_IBU", "name": "布洛芬组", "drug_codes": ["IBU"]},
        {"id": "D_AMX", "name": "阿莫西林组", "drug_codes": ["AMX"]}],
    "event_groups": [
        {"id": "E_RASH", "name": "皮肤事件", "concept_codes": ["RASH"]},
        {"id": "E_GI", "name": "胃肠事件", "concept_codes": ["NAU"]}],
    "thresholds": {"min_cases": 2, "min_prr": 2.0, "min_ror": 2.0,
                   "min_chi_square": 3.841},
}

W1 = ("2026-09-01", "2026-10-01", "2026-10-02T00:00:00")
W2 = ("2026-09-01", "2026-10-01", "2026-11-01T00:00:00")


def line(t=""):
    print(t)


def show_pair(tag, p):
    line(f"  {tag} 四格 a={p['a']} b={p['b']} c={p['c']} d={p['d']}  "
         f"PRR={p['metrics']['prr']} (95%CI {p['metrics']['prr_ci95_lower']}"
         f"~{p['metrics']['prr_ci95_upper']})  "
         f"ROR={p['metrics']['ror']} (95%CI {p['metrics']['ror_ci95_lower']}"
         f"~{p['metrics']['ror_ci95_upper']})  χ²={p['metrics']['chi_square']}"
         f"  -> {p['decision']['decision']}"
         f"{'  [信号修订]' if p['revision_id'] else ''}")


def main():
    data_dir = tempfile.mkdtemp(prefix="signal-demo-")
    clk = FixedClock(datetime(2026, 10, 1, 9, 0, 0))
    app = SignalApplication(data_dir, clock=clk, shard_size=4)
    line("=" * 78)
    line("药物安全信号可复演窗口计算 —— 端到端演示")
    line("=" * 78)

    line("\n[1] 冻结术语归一版本与监测定义")
    app.catalogs.register("terms@2026Q3", CATALOG, actor="analyst-lee")
    app.definitions.create("MON-IBU", SPEC, name="布洛芬皮肤信号监测",
                           actor="analyst-lee", term_catalog_id="terms@2026Q3")
    line("  术语版本 terms@2026Q3 已登记（不可变）；定义 MON-IBU v1 已创建")

    line("\n[2] W1 窗口接收病例（布洛芬×皮疹构成不成比例信号）")
    for i in range(1, 9):
        app.sources.intake(
            f"PV-{i:04d}", "HOSPITAL",
            {"drugs": [{"name": "布洛芬"}], "events": [{"term": "皮疹"}]},
            event_date=f"2026-09-{i:02d}")
    app.sources.intake("PV-1001", "HOSPITAL",
                       {"drugs": [{"name": "布洛芬"}],
                        "events": [{"term": "恶心"}]}, event_date="2026-09-09")
    app.sources.intake("PV-1002", "HOSPITAL",
                       {"drugs": [{"name": "布洛芬"}],
                        "events": [{"term": "恶心"}]}, event_date="2026-09-10")
    app.sources.intake("PV-2001", "ENTERPRISE",
                       {"drugs": [{"name": "阿莫西林"}],
                        "events": [{"term": "皮疹"}]}, event_date="2026-09-11")
    for i in range(3, 8):
        app.sources.intake(
            f"PV-2{i:03d}", "ENTERPRISE",
            {"drugs": [{"name": "阿莫西林"}],
             "events": [{"term": "恶心"}]}, event_date="2026-09-12")
    job1 = app.submit_and_wait(definition_id="MON-IBU", window_start=W1[0],
                               window_end=W1[1], cutoff=W1[2],
                               term_catalog_id="terms@2026Q3")
    run1 = app.results.get_run(job1["run_id"])
    line(f"  作业 {job1['id'][:8]} 分片={job1['shard_count']} "
         f"状态={job1['status']}  来源={run1['total_sources']}")
    line(f"  输入指纹 {run1['input_hash'][:16]}… 输出指纹 "
         f"{run1['output_hash'][:16]}…")
    for p in run1["pairs"]:
        show_pair(f"{p['drug_group_id']}×{p['event_group_id']}", p)
    p1 = next(p for p in run1["pairs"]
              if p["drug_group_id"] == "D_IBU" and p["event_group_id"] == "E_RASH")
    rev1 = p1["revision_id"]
    app.signals.dispose(rev1, "CONFIRMED", "analyst-lee",
                        comment="W1 信号成立，进入重点监测")
    line("  分析员确认 D_IBU×E_RASH 修订 v1 -> CONFIRMED")

    line("\n[3] 同一配置+截止线再次计算：同键去重，同一份结果")
    again = app.runner.submit(definition_id="MON-IBU", window_start=W1[0],
                              window_end=W1[1], cutoff=W1[2],
                              term_catalog_id="terms@2026Q3")
    line(f"  返回同一作业 {again['id'] == job1['id']}，deduped={again['deduped']}，"
         f"run={again['run_id'] == job1['run_id']}")
    verify = app.runner.verify_run(run1["id"])
    line(f"  冻结输入独立重算核验：valid={verify['valid']}，"
         f"哈希一致={verify['hash_match']}")

    line("\n[4] W1 截止线后：迟到随访 + 新病例，进入 W2")
    clk.advance(days=20)
    app.sources.add_version(
        "PV-0001",
        {"drugs": [{"name": "布洛芬"}], "events": [{"term": "恶心"}]},
        change_type="FOLLOWUP")
    line("  PV-0001 迟到随访：皮疹 -> 恶心（撤销旧贡献，计入新事件组）")
    app.sources.intake(
        "PV-0009", "HOTLINE",
        {"drugs": [{"name": "ibuprofeno"}], "events": [{"term": "sarpullido"}]},
        event_date="2026-09-20")
    line("  PV-0009 西语热线报告（归一后=布洛芬×皮疹）进入窗口")

    line("\n[5] W2：增量更新（基线 W1，干净来源复制、脏来源重算）")
    job_inc = app.runner.submit(definition_id="MON-IBU", window_start=W2[0],
                                window_end=W2[1], cutoff=W2[2],
                                term_catalog_id="terms@2026Q3",
                                mode="INCREMENTAL")
    line(f"  增量作业基线={job_inc['prior_run_id'][:8]} "
         f"冻结来源={job_inc['total_sources']} 分片={job_inc['shard_count']}")
    app.runner.run_to_completion(job_inc["id"])
    run2 = app.results.get_run(app.runner.get_job(job_inc["id"])["run_id"])
    for p in run2["pairs"]:
        show_pair(f"{p['drug_group_id']}×{p['event_group_id']}", p)
    line(f"  独立重算核验：{app.runner.verify_run(run2['id'])['valid']}")

    line("\n[6] 可追踪差异（W1 -> W2）")
    diff = app.results.diff_runs(run1["id"], run2["id"])
    for c in diff["case_changes"]["changed"]:
        line(f"  来源 {c['source_id']}: {c['change']} "
             f"{c['old']['event_group_ids']} -> {c['new']['event_group_ids']}")
    for c in diff["case_changes"]["added"]:
        line(f"  新进入 {c['source_id']}: 组={c['event_group_ids']}")
    for s in diff["signal_changes"]["changed"]:
        line(f"  信号 {s['drug_group_id']}×{s['event_group_id']} "
             f"{s['old_counts']} -> {s['new_counts']} ({s['change']})")

    line("\n[7] 信号处置：旧结论保留 / 新修订重新处置 / 过期号拒绝")
    p2 = next(p for p in run2["pairs"]
              if p["drug_group_id"] == "D_IBU" and p["event_group_id"] == "E_RASH")
    rev2 = p2["revision_id"]
    chain = app.signals.get_signal("MON-IBU|D_IBU|E_RASH")
    for r in chain["revisions"]:
        line(f"  v{r['seq_no']} [{r['change_type']}] 状态={r['triage_status']}")
    try:
        app.signals.dispose(rev1, "REJECTED", "analyst-wang")
    except Exception as e:
        line(f"  对旧修订 v1 操作被拒绝：{e.code}（旧 CONFIRMED 保留）")
    try:
        app.signals.dispose(rev2, "CONFIRMED", "analyst-wang",
                            expected_seq_no=1)
    except Exception as e:
        line(f"  过期修订号被拒绝：{e.code}，当前 v{e.extra['current_seq_no']}")
    app.signals.dispose(rev2, "WATCH", "analyst-lee", expected_seq_no=2,
                        comment="迁移后仍显著，标记观察一个窗口")
    line("  v2 以当前修订号处置 -> WATCH")

    line("\n[8] 分片崩溃恢复（演示检查点续跑与不发布半成品）")
    stage = {"killed": False}

    def hook(phase, jid, shard):
        if phase == "after_shard_commit" and shard == 0 and not stage["killed"]:
            stage["killed"] = True
            raise KeyboardInterrupt
    app.runner.crash_hook = hook
    j3 = app.runner.submit(definition_id="MON-IBU", window_start="2026-10-01",
                           window_end="2026-11-01", cutoff="2026-11-02T00:00:00",
                           term_catalog_id="terms@2026Q3")
    try:
        app.runner.tick(j3["id"])  # shard0 提交后“被杀”
    except KeyboardInterrupt:
        pass
    app.runner.crash_hook = None
    line(f"  崩溃时作业={app.runner.get_job(j3['id'])['status']}，"
         f"已发布窗口数={len(app.results.list_runs())}")
    app.runner.recover()
    app.runner.run_to_completion(j3["id"])
    j3f = app.runner.get_job(j3["id"])
    line(f"  恢复续跑后状态={j3f['status']}（已提交分片未重做）")

    line("\n[9] 病例归并与贡献追踪")
    src = app.sources.list_sources()[0]
    case_of = {s["id"]: s["case_id"] for s in app.sources.list_sources()}
    a, b = case_of["PV-0002"], case_of["PV-0003"]
    app.sources.merge_cases(a, b, actor="analyst-lee")
    line(f"  PV-0003 归并进 PV-0002 的病例（血缘闭包更新，四格不重复计数）")
    track = app.results.source_contributions("PV-0001")
    for e in track["contributions"]:
        line(f"  PV-0001 @cutoff {e['cutoff'][:10]}: 事件组="
             f"{e['event_group_ids']} {e['change_from_prior']}")

    line("\n[10] 审计哈希链")
    report = app.verify_audit_chain()
    line(f"  审计事件={report['events']} 链头={report['head'][:16]}… "
         f"valid={report['valid']}")

    line("\n演示完成。所有窗口快照、信号修订、处置结论均为 append-only；")
    line("同一配置与截止线的结果由输入/输出哈希冻结，可离线重算复核。")
    app.shutdown()


if __name__ == "__main__":
    main()
