"""药物安全信号监测 HTTP/JSON 接口（仅依赖标准库 http.server）。

接口分组（与需求一一对应）：

*监测定义*
  POST /api/v1/signals/term-catalogs           登记不可变术语归一版本
  GET  /api/v1/signals/term-catalogs
  GET  /api/v1/signals/term-catalogs/{id}
  POST /api/v1/signals/definitions             创建监测定义（首版本）
  GET  /api/v1/signals/definitions
  GET  /api/v1/signals/definitions/{id}        定义详情（?version_no=）
  POST /api/v1/signals/definitions/{id}/versions  追加定义版本

*病例与归并拆解*
  POST /api/v1/signals/sources                 接收来源首版
  GET  /api/v1/signals/sources
  GET  /api/v1/signals/sources/{id}
  POST /api/v1/signals/sources/{id}/versions   迟到随访/因果修正/退出样本
  GET  /api/v1/signals/cases
  POST /api/v1/signals/merges                  归并 {survivor_case_id,other_case_id}
  POST /api/v1/signals/splits                  拆解 {source_id}

*运行与进度 / 恢复*
  POST /api/v1/signals/runs                    提交窗口计算（同键幂等，自动续跑）
  GET  /api/v1/signals/jobs                    作业列表（?status=&definition_id=）
  GET  /api/v1/signals/jobs/{id}               作业/分片进度
  POST /api/v1/signals/jobs/{id}/run           推进一个分片（或跑到 ?all=true）
  POST /api/v1/signals/recover                 崩溃恢复并可续跑

*窗口结果 / 病例贡献 / 修订差异*
  GET  /api/v1/signals/windows                 窗口列表（?definition_id=）
  GET  /api/v1/signals/windows/{id}            窗口四格/指标/判定/哈希
  GET  /api/v1/signals/windows/{id}/cases      入选病例清单（可按组过滤）
  GET  /api/v1/signals/windows/{id}/verify     冻结输入重算核验
  GET  /api/v1/signals/sources/{id}/contributions  跨窗口贡献追踪
  POST /api/v1/signals/diffs/runs              两窗口差异
  POST /api/v1/signals/diffs/revisions         两修订字段级差异

*信号确认与驳回*
  GET  /api/v1/signals/signals                 信号（?definition_id=&status=）
  GET  /api/v1/signals/signals/{key}           完整修订链（旧结论保留）
  POST /api/v1/signals/signals/revisions/{id}/dispose  确认/驳回/观察（过期号拒绝）

*审计*
  GET  /api/v1/signals/audit                   审计事件
  GET  /api/v1/signals/audit/verify            审计哈希链核验
"""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlparse, parse_qs

from .catalog import CatalogError
from .definitions import DefinitionError
from .results import ResultError
from .runner import RunnerError
from .signals import SignalError
from .sources import SourceError


def _query(qs: str) -> dict:
    return {k: v[-1] for k, v in parse_qs(qs).items()}


def create_server(app, host: str = "127.0.0.1", port: int = 8090) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            pass

        def _send(self, code: int, body):
            data = json.dumps(body, ensure_ascii=False, indent=2,
                              default=str).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _body(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            if not length:
                return {}
            try:
                return json.loads(self.rfile.read(length).decode("utf-8"))
            except json.JSONDecodeError:
                raise _HttpError(400, "BAD_JSON", "请求体不是合法 JSON")

        def do_GET(self):
            self._route("GET")

        def do_POST(self):
            self._route("POST")

        def _route(self, method: str):
            try:
                path = unquote(urlparse(self.path).path.rstrip("/") or "/")
                qs = _query(urlparse(self.path).query)
                body = self._body() if method == "POST" else {}
                parts = [p for p in path.split("/") if p]
                P = parts
                a = app  # 信号应用

                if method == "GET" and path == "/health":
                    return self._send(200, {"status": "ok", "service": "signals"})

                # -- 术语目录 ------------------------------------------------
                if path == "/api/v1/signals/term-catalogs":
                    if method == "POST":
                        r = a.catalogs.register(
                            str(body["id"]), body["spec"], name=body.get("name"),
                            actor=body.get("actor"))
                        return self._send(201, r)
                    return self._send(200, a.catalogs.list_catalogs())
                if (method == "GET" and len(P) == 5
                        and P[:4] == ["api", "v1", "signals", "term-catalogs"]):
                    return self._send(200, a.catalogs.get(P[4]))

                # -- 监测定义 ------------------------------------------------
                if path == "/api/v1/signals/definitions":
                    if method == "POST":
                        r = a.definitions.create(
                            str(body["id"]), body["spec"], name=body.get("name"),
                            actor=body.get("actor"),
                            term_catalog_id=body.get("term_catalog_id"))
                        return self._send(201, r)
                    return self._send(200, a.definitions.list_definitions())
                if (len(P) == 5 and P[:4] == ["api", "v1", "signals", "definitions"]):
                    if method == "GET":
                        vn = qs.get("version_no")
                        return self._send(200, a.definitions.get(
                            P[4], version_no=int(vn) if vn else None))
                if (method == "POST" and len(P) == 6
                        and P[:4] == ["api", "v1", "signals", "definitions"]
                        and P[5] == "versions"):
                    return self._send(201, a.definitions.new_version(
                        P[4], body["spec"], actor=body.get("actor"),
                        term_catalog_id=body.get("term_catalog_id")))

                # -- 来源 ----------------------------------------------------
                if path == "/api/v1/signals/sources":
                    if method == "POST":
                        r = a.sources.intake(
                            str(body["id"]), body.get("origin", "EXTERNAL"),
                            body["payload"], event_date=body.get("event_date"),
                            actor=body.get("actor"),
                            recorded_at=body.get("recorded_at"),
                            idempotency_key=body.get("idempotency_key"))
                        return self._send(201, r)
                    return self._send(200, a.sources.list_sources())
                if (method == "GET" and len(P) == 5
                        and P[:4] == ["api", "v1", "signals", "sources"]):
                    return self._send(200, a.sources.get_source(P[4]))
                if (method == "POST" and len(P) == 6
                        and P[:4] == ["api", "v1", "signals", "sources"]
                        and P[5] == "versions"):
                    r = a.sources.add_version(
                        P[4], body["payload"],
                        change_type=body.get("change_type", "FOLLOWUP"),
                        event_date=body.get("event_date"),
                        actor=body.get("actor"),
                        recorded_at=body.get("recorded_at"),
                        idempotency_key=body.get("idempotency_key"))
                    return self._send(201, r)
                if (method == "GET" and len(P) == 6
                        and P[:4] == ["api", "v1", "signals", "sources"]
                        and P[5] == "contributions"):
                    return self._send(200, a.results.source_contributions(
                        P[4], definition_id=qs.get("definition_id")))

                # -- 病例/归并/拆解 ------------------------------------------
                if method == "GET" and path == "/api/v1/signals/cases":
                    return self._send(200, a.sources.list_cases())
                if method == "POST" and path == "/api/v1/signals/merges":
                    return self._send(200, a.sources.merge_cases(
                        body["survivor_case_id"], body["other_case_id"],
                        actor=body.get("actor"),
                        effective_at=body.get("effective_at")))
                if method == "POST" and path == "/api/v1/signals/splits":
                    return self._send(201, a.sources.split_source(
                        body["source_id"], actor=body.get("actor"),
                        effective_at=body.get("effective_at"),
                        expected_case_id=body.get("expected_case_id")))

                # -- 作业/运行 -----------------------------------------------
                if method == "POST" and path == "/api/v1/signals/runs":
                    job = a.runner.submit(
                        definition_id=str(body["definition_id"]),
                        window_start=str(body["window_start"]),
                        window_end=str(body["window_end"]),
                        cutoff=str(body["cutoff"]),
                        term_catalog_id=body.get("term_catalog_id"),
                        definition_version_no=body.get("definition_version_no"),
                        mode=body.get("mode", "FULL"),
                        requested_by=body.get("actor"),
                        shard_size=body.get("shard_size"))
                    if job["status"] != "COMPLETED" and body.get("wait", True):
                        a.runner.run_to_completion(job["id"])
                        job = a.runner.get_job(job["id"])
                    return self._send(201 if not job.get("deduped") else 200, job)
                if method == "GET" and path == "/api/v1/signals/jobs":
                    return self._send(200, a.runner.list_jobs(
                        status=qs.get("status"), definition_id=qs.get("definition_id")))
                if (len(P) == 5 and P[:4] == ["api", "v1", "signals", "jobs"]):
                    if method == "GET":
                        return self._send(200, a.runner.get_job(P[4]))
                if (method == "POST" and len(P) == 6
                        and P[:4] == ["api", "v1", "signals", "jobs"]
                        and P[5] == "run"):
                    if qs.get("all") == "true" or body.get("all"):
                        return self._send(200, a.runner.run_to_completion(
                            P[4], max_shards=body.get("max_shards")))
                    return self._send(200, a.runner.tick(P[4]))
                if method == "POST" and path == "/api/v1/signals/recover":
                    rec = a.runner.recover()
                    if body.get("resume"):
                        for j in a.runner.list_jobs(status="PENDING"):
                            a.runner.run_to_completion(j["id"])
                    return self._send(200, rec)

                # -- 窗口结果 ------------------------------------------------
                if method == "GET" and path == "/api/v1/signals/windows":
                    return self._send(200, a.results.list_runs(
                        definition_id=qs.get("definition_id")))
                if (method == "GET" and len(P) == 5
                        and P[:4] == ["api", "v1", "signals", "windows"]):
                    return self._send(200, a.results.get_run(P[4]))
                if (method == "GET" and len(P) == 6
                        and P[:4] == ["api", "v1", "signals", "windows"]
                        and P[5] == "cases"):
                    return self._send(200, a.results.run_cases(
                        P[4], included_only=qs.get("included_only", "1") != "0",
                        drug_group_id=qs.get("drug_group_id"),
                        event_group_id=qs.get("event_group_id")))
                if (method == "GET" and len(P) == 6
                        and P[:4] == ["api", "v1", "signals", "windows"]
                        and P[5] == "verify"):
                    return self._send(200, a.runner.verify_run(P[4]))

                # -- 差异 ----------------------------------------------------
                if method == "POST" and path == "/api/v1/signals/diffs/runs":
                    return self._send(200, a.results.diff_runs(
                        body["old_run_id"], body["new_run_id"]))
                if method == "POST" and path == "/api/v1/signals/diffs/revisions":
                    return self._send(200, a.results.revision_diff(
                        body["old_revision_id"], body["new_revision_id"]))

                # -- 信号处置 ------------------------------------------------
                if method == "GET" and path == "/api/v1/signals/signals":
                    return self._send(200, a.signals.list_signals(
                        definition_id=qs.get("definition_id"),
                        run_id=qs.get("run_id"), status=qs.get("status"),
                        include_all_revisions=qs.get("all_revisions") == "1"))
                if (len(P) == 5 and P[:4] == ["api", "v1", "signals", "signals"]):
                    if method == "GET":
                        return self._send(200, a.signals.get_signal(P[4]))
                if (method == "POST" and len(P) == 7
                        and P[:4] == ["api", "v1", "signals", "signals"]
                        and P[4] == "revisions" and P[6] == "dispose"):
                    # /signals/signals/revisions/{id}/dispose
                    return self._send(200, a.signals.dispose(
                        P[5], body["decision"], body["analyst"],
                        comment=body.get("comment"),
                        expected_seq_no=body.get("expected_seq_no")))

                # -- 审计 ----------------------------------------------------
                if method == "GET" and path == "/api/v1/signals/audit":
                    return self._send(200, a.audit_events(
                        event_type=qs.get("event_type"), actor=qs.get("actor")))
                if method == "GET" and path == "/api/v1/signals/audit/verify":
                    return self._send(200, a.verify_audit_chain())

                return self._send(404, {"error": {"code": "NOT_FOUND",
                                                  "message": path}})
            except _HttpError as e:
                return self._send(e.status, {"error": {"code": e.code,
                                                       "message": str(e)}})
            except (CatalogError, DefinitionError, SourceError, RunnerError,
                    SignalError, ResultError) as e:
                code = getattr(e, "code", "DOMAIN_ERROR")
                status = 409 if code in ("STALE_REVISION", "ID_CONFLICT") else 400
                if code in ("CATALOG_NOT_FOUND", "DEFINITION_NOT_FOUND",
                            "VERSION_NOT_FOUND", "SOURCE_NOT_FOUND",
                            "CASE_NOT_FOUND", "RUN_NOT_FOUND", "JOB_NOT_FOUND",
                            "REVISION_NOT_FOUND", "SIGNAL_NOT_FOUND",
                            "PAIR_NOT_FOUND"):
                    status = 404
                if code in ("ALREADY_DISPOSED",):
                    status = 409
                payload = {"error": {"code": code, "message": e.message}}
                extra = getattr(e, "extra", None)
                if extra:
                    payload["error"]["details"] = extra
                return self._send(status, payload)

    class _HttpError(Exception):
        def __init__(self, status, code, message):
            super().__init__(message)
            self.status = status
            self.code = code

    return ThreadingHTTPServer((host, port), Handler)


def serve(app, host: str = "127.0.0.1", port: int = 8090):
    server = create_server(app, host, port)
    print(f"信号监测服务监听 http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
