"""规则化报送 HTTP/JSON 接口（仅依赖标准库 http.server）。

接口：
  POST /api/v1/intake                 来源报文接收（幂等）
  GET  /api/v1/cases                  病例工作台
  GET  /api/v1/cases/{id}             病例详情（版本时间线）
  GET  /api/v1/cases/{id}/timeline    病例审计时间线（含 seq/哈希链）
  POST /api/v1/cases/{id}/followup    随访材料（产生新版本）
  POST /api/v1/cases/{id}/split       拆分来源到新病例
  GET  /api/v1/duplicates             待复核疑似重复簇
  POST /api/v1/duplicates/{id}/confirm   专员确认合并
  POST /api/v1/duplicates/{id}/reject    专员否决
  GET  /api/v1/reports                报告列表（?status=QUEUED）
  GET  /api/v1/reports/due            待报送/逾期视图
  POST /api/v1/reports/dispatch       同步派发待发送报告
  GET  /api/v1/reports/{id}           报告详情（含监管回执）
  POST /api/v1/reports/{id}/requeue   人工重置失败报告
  POST /api/v1/exports                发起证据包导出（幂等：case+version+key）
  GET  /api/v1/exports                导出任务列表（?case_id=）
  GET  /api/v1/exports/{id}           导出任务状态/进度
  GET  /api/v1/exports/{id}/download  下载证据包（.tar 字节流）
  POST /api/v1/exports/{id}/verify    服务端离线核验（逐项结果留档）
  GET  /api/v1/exports/{id}/verifications  某任务历史核验结果

双人会签（药物安全决定）：
  POST /api/v1/reviews                发起不可变评审单（冻结最新修订号）
  GET  /api/v1/reviews/todos          待办领取工作台（?reviewer=）
  GET  /api/v1/reviews/history        决定历史（?report_id=&case_id=&status=）
  GET  /api/v1/reviews/audit          审计查询（?sheet_id=&event_type=&actor=）
  GET  /api/v1/reviews/revisions      修订号/失效原因历史
  GET  /api/v1/reviews/approvals      会签放行记录（?report_id=）
  POST /api/v1/rules/recalculate      规则重新计算（作废旧评审单）

药物安全法规规则治理（登记/试算/受控启用/回滚/定时/恢复/审计）：
  POST /api/v1/rule-packages          登记不可变规则包（签名/依赖/区间校验）
  GET  /api/v1/rule-packages          规则包列表（?jurisdiction=）
  GET  /api/v1/rule-packages/{id}     规则包详情
  POST /api/v1/rule-packages/sign     用注册表密钥对内容签名（登记辅助）
  GET  /api/v1/rules/current          各辖区当前生效规则版本（?jurisdiction=）
  POST /api/v1/rules/trials           创建试算任务（冻结历史快照集合）
  GET  /api/v1/rules/trials           试算任务列表
  POST /api/v1/rules/trials/{id}/run  续跑试算（检查点）
  GET  /api/v1/rules/trials/{id}/progress  进度 + 受影响病例集合
  GET  /api/v1/rules/trials/{id}/diffs     旧/候选决定差异（?changed_only=）
  POST /api/v1/rules/activate         受控启用（绑定当前版本，乐观并发）
  POST /api/v1/rules/rollback         回滚到历史版本（缺省=上一版本）
  GET  /api/v1/rules/activations      启用/回滚历史
  GET  /api/v1/rules/jobs             启用作业列表（?jurisdiction=）
  POST /api/v1/rules/jobs/{id}/run    续跑启用作业（检查点）
  GET  /api/v1/rules/corrections      已发送报告的更正候选（?report_id=）
  POST /api/v1/rules/schedules        创建定时生效计划
  GET  /api/v1/rules/schedules        定时计划列表（?status=）
  POST /api/v1/rules/schedules/{id}/cancel  取消定时计划
  POST /api/v1/rules/recover          崩溃恢复（重置中断任务）+ 续跑
  GET  /api/v1/rules/audit            治理审计事件（?event_type=&jurisdiction=）
  GET  /api/v1/rules/audit/verify     核验治理审计哈希链
  GET  /api/v1/reviews/{id}           评审单详情（冻结项/意见/事件）
  POST /api/v1/reviews/{id}/claim     领取待办（{reviewer}）
  POST /api/v1/reviews/{id}/release   释放租约（{reviewer}）
  POST /api/v1/reviews/{id}/approve   赞成签署（{reviewer,comment}）
  POST /api/v1/reviews/{id}/veto      否决（{reviewer,reason}）
  POST /api/v1/reviews/{id}/withdraw  撤回赞成（{reviewer}）
  POST /api/v1/reviews/{id}/expire    释放到期租约（维护/服务恢复）
  GET  /health
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from .service import DomainError


def _query(qs: str) -> dict:
    return {k: v[-1] for k, v in parse_qs(qs).items()}


def create_server(app, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            pass

        def _send(self, code: int, body: dict | list):
            data = json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _send_bytes(self, code: int, data: bytes, content_type: str,
                        filename: str | None = None):
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            if filename:
                self.send_header("Content-Disposition",
                                 f'attachment; filename="{filename}"')
            self.end_headers()
            self.wfile.write(data)

        def _body(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            if not length:
                return {}
            raw = self.rfile.read(length)
            try:
                return json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                raise DomainError("BAD_JSON", "请求体不是合法 JSON")

        def do_GET(self):
            self._route("GET")

        def do_POST(self):
            self._route("POST")

        def _route(self, method: str):
            try:
                path = urlparse(self.path).path.rstrip("/") or "/"
                qs = urlparse(self.path).query
                body = self._body() if method == "POST" else {}
                parts = [p for p in path.split("/") if p]

                if method == "GET" and path == "/health":
                    return self._send(200, {"status": "ok"})

                if method == "POST" and path == "/api/v1/intake":
                    return self._send(201, app.cases.intake(
                        body["source_system"], str(body["external_id"]),
                        body.get("payload") or body, body.get("received_at")))

                if method == "GET" and path == "/api/v1/cases":
                    return self._send(200, app.cases.workbench())

                if method == "GET" and len(parts) == 4 and parts[:3] == ["api", "v1", "cases"]:
                    return self._send(200, app.cases.get_case(parts[3]))

                if method == "GET" and len(parts) == 5 and \
                        parts[:3] == ["api", "v1", "cases"] and parts[4] == "timeline":
                    return self._send(200, {
                        "case_id": parts[3],
                        "timeline": app.cases.timeline(parts[3])})

                if method == "POST" and len(parts) == 5 and \
                        parts[:3] == ["api", "v1", "cases"] and parts[4] == "followup":
                    return self._send(201, app.cases.add_followup(
                        parts[3], body.get("content") or body,
                        body.get("received_at"), body.get("actor")))

                if method == "POST" and len(parts) == 5 and \
                        parts[:3] == ["api", "v1", "cases"] and parts[4] == "split":
                    return self._send(201, app.cases.split_source(
                        parts[3], body["source_id"], body.get("actor")))

                if method == "GET" and path == "/api/v1/duplicates":
                    status = "PROPOSED"
                    for kv in qs.split("&"):
                        if kv.startswith("status="):
                            status = kv.split("=", 1)[1]
                    return self._send(200, app.cases.list_clusters(status))

                if method == "POST" and len(parts) == 5 and \
                        parts[:3] == ["api", "v1", "duplicates"] and parts[4] == "confirm":
                    return self._send(200, app.cases.confirm_cluster(
                        parts[3], body.get("survivor_case_id"), body.get("actor")))

                if method == "POST" and len(parts) == 5 and \
                        parts[:3] == ["api", "v1", "duplicates"] and parts[4] == "reject":
                    return self._send(200, app.cases.reject_cluster(
                        parts[3], body.get("reason"), body.get("actor")))

                if method == "GET" and path == "/api/v1/reports/due":
                    return self._send(200, app.reporting.due())

                if method == "POST" and path == "/api/v1/reports/dispatch":
                    return self._send(200, app.reporting.pump(
                        body.get("max_batches", 100),
                        allow_ungated=bool(body.get("allow_ungated", False))))

                if method == "GET" and path == "/api/v1/reports":
                    status = None
                    for kv in qs.split("&"):
                        if kv.startswith("status="):
                            status = kv.split("=", 1)[1]
                    return self._send(200, app.reporting.list_reports(status))

                if method == "GET" and len(parts) == 4 and \
                        parts[:3] == ["api", "v1", "reports"]:
                    return self._send(200, app.reporting.get_report(parts[3]))

                if method == "POST" and len(parts) == 5 and \
                        parts[:3] == ["api", "v1", "reports"] and parts[4] == "requeue":
                    return self._send(200, app.reporting.requeue(parts[3]))

                # -- 双人会签评审 ---------------------------------------------
                if method == "POST" and path == "/api/v1/reviews":
                    report_ids = body.get("report_ids")
                    if report_ids is not None and not isinstance(report_ids, list):
                        raise DomainError("BAD_FIELD", "report_ids 必须是数组")
                    sheet = app.reviews.create_sheet(
                        [str(x) for x in report_ids] if report_ids else None,
                        requested_by=body.get("requested_by"), title=body.get("title"))
                    return self._send(201, sheet)

                if method == "GET" and path == "/api/v1/reviews/todos":
                    q = _query(qs)
                    return self._send(200, app.reviews.list_todos(
                        reviewer=q.get("reviewer")))

                if method == "GET" and path == "/api/v1/reviews/history":
                    q = _query(qs)
                    return self._send(200, app.reviews.decision_history(
                        report_id=q.get("report_id"), case_id=q.get("case_id"),
                        status=q.get("status"),
                        limit=int(q.get("limit", 100))))

                if method == "GET" and path == "/api/v1/reviews/audit":
                    q = _query(qs)
                    return self._send(200, app.reviews.audit_trail(
                        sheet_id=q.get("sheet_id"), event_type=q.get("event_type"),
                        actor=q.get("actor"), limit=int(q.get("limit", 200))))

                if method == "GET" and path == "/api/v1/reviews/revisions":
                    q = _query(qs)
                    return self._send(200, app.reviews.revision_history(
                        limit=int(q.get("limit", 100))))

                if method == "GET" and path == "/api/v1/reviews/approvals":
                    q = _query(qs)
                    return self._send(200, app.reviews.dispatch_approvals(
                        q.get("report_id")))

                if method == "POST" and path == "/api/v1/rules/recalculate":
                    return self._send(200, app.cases.recalculate_rules(
                        body.get("actor")))

                if method == "GET" and len(parts) == 4 and parts[:3] == ["api", "v1", "reviews"]:
                    return self._send(200, app.reviews.get_sheet(parts[3]))

                _REVIEW_ACTS = {"claim", "release", "approve", "veto",
                                "withdraw", "expire"}
                if method == "POST" and len(parts) == 5 and \
                        parts[:3] == ["api", "v1", "reviews"] and parts[4] in _REVIEW_ACTS:
                    act = parts[4]
                    if act == "claim":
                        result = app.reviews.claim(parts[3], body["reviewer"])
                    elif act == "release":
                        result = app.reviews.release(parts[3], body["reviewer"])
                    elif act == "approve":
                        result = app.reviews.approve(
                            parts[3], body["reviewer"], body.get("comment"))
                    elif act == "veto":
                        result = app.reviews.veto(
                            parts[3], body["reviewer"], body.get("reason"))
                    elif act == "withdraw":
                        result = app.reviews.withdraw(parts[3], body["reviewer"])
                    else:
                        result = {"expired": app.reviews.expire_leases()}
                    return self._send(200, result)

                # -- 证据包导出 / 下载 / 核验 --------------------------------
                if method == "POST" and path == "/api/v1/exports":
                    case_id = body.get("case_id")
                    if not case_id:
                        raise DomainError("MISSING_FIELD", "缺少 case_id")
                    task = app.exports.request_export(
                        case_id, version_id=body.get("version_id"),
                        requested_by=body.get("requested_by"),
                        idempotency_key=body.get("idempotency_key"))
                    wait = bool(body.get("wait", True))
                    if wait and task["status"] in ("PENDING", "BUILDING"):
                        app.exports.run_until_idle()
                        task = app.exports.get_task(task["task_id"])
                    return self._send(201 if task["status"] == "COMPLETED" else 202,
                                      task)

                if method == "GET" and path == "/api/v1/exports":
                    case_id = None
                    for kv in qs.split("&"):
                        if kv.startswith("case_id="):
                            case_id = kv.split("=", 1)[1]
                    return self._send(200, app.exports.list_tasks(case_id))

                if method == "GET" and len(parts) == 4 and parts[:3] == ["api", "v1", "exports"]:
                    return self._send(200, app.exports.get_task(parts[3]))

                if method == "GET" and len(parts) == 5 and parts[:3] == ["api", "v1", "exports"] \
                        and parts[4] == "download":
                    pkg_path, task = app.exports.package_path(parts[3])
                    data = pkg_path.read_bytes()
                    return self._send_bytes(
                        200, data, "application/x-tar",
                        filename=f"evidence-{task['case_id'][:8]}-{task['package_root'][:12]}.tar")

                if method == "POST" and len(parts) == 5 and parts[:3] == ["api", "v1", "exports"] \
                        and parts[4] == "verify":
                    report = app.exports.verify_task(
                        parts[3],
                        trusted_key_fingerprint=body.get("trusted_key_fingerprint"))
                    return self._send(200 if report["valid"] else 422, report)

                if method == "GET" and len(parts) == 5 and parts[:3] == ["api", "v1", "exports"] \
                        and parts[4] == "verifications":
                    return self._send(200, app.exports.verification_runs(parts[3]))

                # -- 药物安全法规规则治理 -------------------------------------
                gov = app.rules_gov

                if method == "POST" and path == "/api/v1/rule-packages/sign":
                    from .rule_pkg import sign_content
                    return self._send(200, {
                        "signature": sign_content(
                            body["content"], body.get("secret", ""))})

                if method == "POST" and path == "/api/v1/rule-packages":
                    pkg = gov.register_package(
                        body["content"], body["signature"],
                        signer_id=body.get("signer_id", "demo-regulator"),
                        registered_by=body.get("registered_by"))
                    return self._send(201 if not pkg.get("deduped") else 200, pkg)

                if method == "GET" and path == "/api/v1/rule-packages":
                    q = _query(qs)
                    return self._send(200, gov.list_packages(q.get("jurisdiction")))

                if method == "GET" and len(parts) == 4 and parts[:3] == ["api", "v1", "rule-packages"]:
                    pid = parts[3]
                    if pid == "legacy":
                        from .rule_pkg import legacy_view
                        return self._send(200, legacy_view())
                    return self._send(200, gov.get_package(package_row_id=pid))

                if method == "GET" and path == "/api/v1/rules/current":
                    q = _query(qs)
                    if q.get("jurisdiction"):
                        return self._send(200, gov.current_version(q["jurisdiction"]))
                    return self._send(200, gov.list_current_versions())

                if method == "POST" and path == "/api/v1/rules/trials":
                    cand = body["candidate"]
                    trial = gov.create_trial(
                        cand["package_id"], str(cand["version"]),
                        selector=body.get("selector"),
                        baseline_package_id=(body.get("baseline") or {}).get("package_id"),
                        baseline_version=(body.get("baseline") or {}).get("version"),
                        idempotency_key=body.get("idempotency_key"),
                        requested_by=body.get("requested_by"),
                        shard_size=body.get("shard_size"))
                    if body.get("wait", True):
                        trial = gov.run_trial_until_done(trial["trial_id"])
                    return self._send(201 if trial["status"] == "COMPLETED" else 202, trial)

                if method == "GET" and path == "/api/v1/rules/trials":
                    q = _query(qs)
                    return self._send(200, gov.list_trials(q.get("jurisdiction")))

                if method == "GET" and len(parts) == 5 and parts[:4] == ["api", "v1", "rules", "trials"]:
                    return self._send(200, gov.get_trial(parts[4]))

                if method == "POST" and len(parts) == 6 and parts[:4] == ["api", "v1", "rules", "trials"] \
                        and parts[5] == "run":
                    return self._send(200, gov.run_trial(parts[4],
                                       max_shards=body.get("max_shards")))

                if method == "GET" and len(parts) == 6 and parts[:4] == ["api", "v1", "rules", "trials"] \
                        and parts[5] == "progress":
                    return self._send(200, gov.trial_progress(parts[4]))

                if method == "GET" and len(parts) == 6 and parts[:4] == ["api", "v1", "rules", "trials"] \
                        and parts[5] == "diffs":
                    q = _query(qs)
                    return self._send(200, gov.trial_diffs(
                        parts[4],
                        changed_only=q.get("changed_only", "true").lower() != "false",
                        limit=int(q.get("limit", 500))))

                if method == "POST" and path == "/api/v1/rules/activate":
                    target = body["target"]
                    res = gov.activate(
                        target["package_id"], str(target["version"]),
                        actor=body.get("actor"),
                        expected_package_id=body.get("expected_package_id"),
                        expected_version=body.get("expected_version"),
                        idempotency_key=body.get("idempotency_key"),
                        wait=bool(body.get("wait", True)))
                    return self._send(200, res)

                if method == "POST" and path == "/api/v1/rules/rollback":
                    target = body.get("target") or {}
                    res = gov.rollback(
                        target_package_id=target.get("package_id"),
                        target_version=target.get("version"),
                        actor=body.get("actor"),
                        expected_package_id=body.get("expected_package_id"),
                        expected_version=body.get("expected_version"),
                        idempotency_key=body.get("idempotency_key"),
                        wait=bool(body.get("wait", True)))
                    return self._send(200, res)

                if method == "GET" and path == "/api/v1/rules/activations":
                    q = _query(qs)
                    return self._send(200, gov.list_activations(q.get("jurisdiction")))

                if method == "GET" and path == "/api/v1/rules/jobs":
                    q = _query(qs)
                    return self._send(200, gov.list_jobs(q.get("jurisdiction")))

                if method == "GET" and len(parts) == 5 and parts[:4] == ["api", "v1", "rules", "jobs"]:
                    return self._send(200, gov.get_job(parts[4]))

                if method == "POST" and len(parts) == 6 and parts[:4] == ["api", "v1", "rules", "jobs"] \
                        and parts[5] == "run":
                    return self._send(200, gov.run_job(parts[4],
                                       max_shards=body.get("max_shards")))

                if method == "GET" and path == "/api/v1/rules/corrections":
                    q = _query(qs)
                    return self._send(200, gov.correction_candidates(
                        report_id=q.get("report_id"), jurisdiction=q.get("jurisdiction")))

                if method == "POST" and path == "/api/v1/rules/schedules":
                    target = body["target"]
                    return self._send(201, gov.schedule(
                        target["package_id"], str(target["version"]),
                        body["effective_at"], requested_by=body.get("requested_by")))

                if method == "GET" and path == "/api/v1/rules/schedules":
                    q = _query(qs)
                    return self._send(200, gov.list_schedules(q.get("status")))

                if method == "POST" and len(parts) == 6 and parts[:4] == ["api", "v1", "rules", "schedules"] \
                        and parts[5] == "cancel":
                    return self._send(200, gov.cancel_schedule(parts[4],
                                          actor=body.get("actor")))

                if method == "POST" and path == "/api/v1/rules/recover":
                    recovery = gov.recover()
                    if body.get("resume", True):
                        recovery["resumed"] = gov.resume_pending(
                            max_shards=body.get("max_shards"))
                    return self._send(200, recovery)

                if method == "GET" and path == "/api/v1/rules/audit/verify":
                    report = gov.verify_audit_chain()
                    return self._send(200 if report["valid"] else 422, report)

                if method == "GET" and path == "/api/v1/rules/audit":
                    q = _query(qs)
                    return self._send(200, gov.audit_trail(
                        event_type=q.get("event_type"), jurisdiction=q.get("jurisdiction"),
                        limit=int(q.get("limit", 500))))

                raise DomainError("NOT_FOUND", f"无此接口: {method} {path}", 404)
            except DomainError as e:
                self._send(e.status, {"error": e.code, "message": str(e)})
            except KeyError as e:
                self._send(400, {"error": "MISSING_FIELD", "message": f"缺少字段: {e}"})
            except Exception as e:  # noqa: BLE001
                self._send(500, {"error": "INTERNAL", "message": str(e)})

    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    return server


def serve(app, host: str = "127.0.0.1", port: int = 8080) -> None:
    server = create_server(app, host, port)
    print(f"PV service listening on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        app.shutdown()
