"""端到端：四格计数、可复现性、冻结版本、信号触发。"""
import unittest

from tests.signals_base import BaseSignalTest


def make_signal_dataset(t):
    """D_IBU×E_RASH 强信号：8 布洛芬皮疹 + 背景对照。"""
    for i in range(1, 9):
        t.source(f"r{i}", ["布洛芬"], ["皮疹"], f"2026-09-{i:02d}")
    t.source("g1", ["布洛芬"], ["恶心"], "2026-09-09")
    t.source("g2", ["布洛芬"], ["恶心"], "2026-09-10")
    t.source("g3", ["阿莫西林"], ["皮疹"], "2026-09-11")
    for i, sid in enumerate(["g4", "g5", "g6", "g7", "g8"]):
        t.source(sid, ["阿莫西林"], ["恶心"], f"2026-09-{12+i:02d}")


class TestFourFoldE2E(BaseSignalTest):
    def test_counts_and_signal(self):
        make_signal_dataset(self)
        run = self.run_of(self.submit_full())
        p = self.pair(run, "D_IBU", "E_RASH")
        # a=8（布洛芬皮疹） b=2（布洛芬非皮疹：恶心） c=1（皮疹非布洛芬） d=5
        self.assertEqual((p["a"], p["b"], p["c"], p["d"]), (8, 2, 1, 5))
        self.assertEqual(p["n"], 16)
        self.assertEqual(p["decision"]["decision"], "SIGNAL")
        self.assertTrue(p["decision"]["is_signal"])
        self.assertEqual(len(p["a_source_ids"]), 8)
        # 非信号对不起修订
        p2 = self.pair(run, "D_WAR", "E_BLD")
        self.assertEqual(p2["a"], 0)
        self.assertIsNone(p2["revision_id"])

    def test_a_cell_case_list(self):
        make_signal_dataset(self)
        run = self.run_of(self.submit_full())
        cases = self.app.results.run_cases(
            run["id"], drug_group_id="D_IBU", event_group_id="E_RASH")
        self.assertEqual({c["source_id"] for c in cases},
                         {f"r{i}" for i in range(1, 9)})
        # 全部入选清单含背景
        all_cases = self.app.results.run_cases(run["id"])
        self.assertEqual(len(all_cases), 16)


class TestReproducibility(BaseSignalTest):
    def test_same_config_and_cutoff_same_job(self):
        make_signal_dataset(self)
        j1 = self.submit_full()
        j2 = self.submit_full()
        self.assertEqual(j1["id"], j2["id"])
        self.assertTrue(j2["deduped"])
        self.assertEqual(j1["run_id"], j2["run_id"])

    def test_rerun_recompute_matches(self):
        make_signal_dataset(self)
        run = self.run_of(self.submit_full())
        v = self.app.runner.verify_run(run["id"])
        self.assertTrue(v["valid"])
        self.assertEqual(v["recomputed_output_hash"], run["output_hash"])

    def test_frozen_cutoff_hides_late_versions(self):
        make_signal_dataset(self)
        run1 = self.run_of(self.submit_full(self.W1))
        p1 = self.pair(run1, "D_IBU", "E_RASH")
        a1 = p1["a"]
        # 截止线后到达的随访：r1 皮疹 -> 恶心
        self.clock.advance(days=20)
        self.followup("r1", ["布洛芬"], ["恶心"])
        # 用旧截止线重算：新版本不可见，结果逐字节一致
        run_old = self.run_of(self.submit_full(self.W1))
        self.assertEqual(run_old["output_hash"], run1["output_hash"])
        self.assertEqual(self.pair(run_old, "D_IBU", "E_RASH")["a"], a1)
        # 新截止线窗口：旧贡献撤销、新贡献计入
        run2 = self.run_of(self.submit_full(self.W2))
        self.assertNotEqual(run2["output_hash"], run1["output_hash"])
        self.assertEqual(self.pair(run2, "D_IBU", "E_RASH")["a"], a1 - 1)
        self.assertEqual(self.pair(run2, "D_IBU", "E_GI")["a"], 3)

    def test_frozen_term_catalog_not_affected_by_new_version(self):
        make_signal_dataset(self)
        run1 = self.run_of(self.submit_full(self.W1))
        # 登记新版本目录（每个概念新增一个新版本专属别名）——旧 run 哈希不变
        old_spec = self.app.catalogs.get(self.CATALOG_ID)["spec"]["concepts"]
        new_cat = {"concepts": [
            dict(c, terms=list(c["terms"]) + [f"{c['code'].lower()}-v2-term"])
            for c in old_spec]}
        self.app.catalogs.register("terms@2", new_cat)
        run_again = self.run_of(self.submit_full(self.W1))
        self.assertEqual(run_again["output_hash"], run1["output_hash"])

    def test_definition_version_frozen(self):
        make_signal_dataset(self)
        run1 = self.run_of(self.submit_full(self.W1))
        # 追加定义新版本（阈值放宽）；不指定版本号取最新 -> 新结果
        relaxed = {
            "drug_groups": [{"id": "D_IBU", "drug_codes": ["IBU"]}],
            "event_groups": [{"id": "E_RASH", "concept_codes": ["RASH"]}],
            "thresholds": {"min_cases": 1},
        }
        self.app.definitions.new_version(
            self.DEFINITION_ID, relaxed, term_catalog_id=self.CATALOG_ID)
        # 旧版本号复算仍一致
        run_v1 = self.run_of(self.submit_full(self.W1, definition_version_no=1))
        self.assertEqual(run_v1["output_hash"], run1["output_hash"])
        # 作业冻结的是具体版本 id，组对数量随定义版本不同
        self.assertEqual(len(run1["pairs"]), 9)

    def test_incremental_staging_rows_match_full_build(self):
        """增量暂存行（复制/重算混合）与纯函数全量构建逐行一致。"""
        make_signal_dataset(self)
        self.run_of(self.submit_full(self.W1))
        self.clock.advance(days=20)
        self.followup("r1", ["布洛芬"], ["恶心"])
        self.source("n1", ["布洛芬"], ["皮疹"], "2026-09-20")
        job_inc = self.app.runner.submit(
            definition_id=self.DEFINITION_ID, window_start=self.W2[0],
            window_end=self.W2[1], cutoff=self.W2[2],
            term_catalog_id=self.CATALOG_ID, mode="INCREMENTAL")
        self.app.runner.run_to_completion(job_inc["id"])
        job_inc = self.app.runner.get_job(job_inc["id"])
        # 发布后 run_cases 与 verify_run 的独立重算逐行哈希一致
        v = self.app.runner.verify_run(job_inc["run_id"])
        self.assertTrue(v["valid"], v["mismatches"])
        # 脏分片（含 r1/n1）走重算，干净来源走复制；两种路径产物相同
        run_id = job_inc["run_id"]
        from pv_service.signals.db import connect as _c
        conn = _c(self.app.db_path)
        try:
            clean_sources = [r["source_id"] for r in conn.execute(
                "SELECT source_id FROM sig_job_sources WHERE job_id=? AND dirty=0",
                (job_inc["id"],))]
            self.assertTrue(clean_sources)  # 存在复制路径来源
            copied = conn.execute(
                "SELECT COUNT(*) AS n FROM sig_job_case_rows j "
                "JOIN sig_run_cases r ON r.run_id=? AND r.source_id=j.source_id "
                "AND r.row_hash=j.row_hash WHERE j.job_id=?",
                (job_inc["prior_run_id"], job_inc["id"])).fetchone()["n"]
            self.assertGreater(copied, 0)
        finally:
            conn.close()

    def test_sharding_independent_of_shard_size(self):
        make_signal_dataset(self)
        for size in (1, 3, 50, 1000):
            # 用不同截止线窗口规避同键去重（输入相同仍需同输出），
            # 这里直接验证 verify_run 对极小分片重建的结果一致。
            job = self.submit_full(self.W1, shard_size=size)
            run = self.run_of(job)
            v = self.app.runner.verify_run(run["id"])
            self.assertTrue(v["valid"], f"shard_size={size} 核验失败")

    def test_incremental_matches_full_output_hash(self):
        make_signal_dataset(self)
        run_full1 = self.run_of(self.submit_full(self.W1))
        # 迟到随访 + 新病例后，新窗口：先增量（基线=W1 全量），再全量同键
        self.clock.advance(days=20)
        self.followup("r1", ["布洛芬"], ["恶心"])
        self.source("n1", ["布洛芬"], ["皮疹"], "2026-09-20")
        job_inc = self.app.runner.submit(
            definition_id=self.DEFINITION_ID, window_start=self.W2[0],
            window_end=self.W2[1], cutoff=self.W2[2],
            term_catalog_id=self.CATALOG_ID, mode="INCREMENTAL")
        self.assertEqual(job_inc["prior_run_id"], run_full1["id"])
        self.app.runner.run_to_completion(job_inc["id"])
        run_inc = self.run_of(self.app.runner.get_job(job_inc["id"]))
        # 同计算键的全量提交命中同一作业（一组信号修订）
        job_full2 = self.submit_full(self.W2, mode="FULL")
        self.assertTrue(job_full2.get("deduped"))
        self.assertEqual(job_full2["run_id"], run_inc["id"])
        # 纯函数独立重算核验增量产出与冻结输入一致
        self.assertTrue(self.app.runner.verify_run(run_inc["id"])["valid"])
        self.assertEqual(run_inc["total_sources"], run_full1["total_sources"] + 1)


if __name__ == "__main__":
    unittest.main()
