"""信号监测应用装配：目录/定义/来源/运行器/信号/结果服务的统一入口。"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta

from ..service import Clock
from . import audit as sig_audit
from .catalog import CatalogService
from .db import connect, initialize
from .definitions import DefinitionService
from .results import ResultService
from .runner import SignalRunner
from .signals import SignalService
from .sources import SourceService


class FixedClock(Clock):
    """可推进的确定性时钟（测试/演示保证截止日期与 recorded_at 可复现）。"""

    def __init__(self, start: datetime):
        self._now = start

    def now(self) -> datetime:
        return self._now

    def advance(self, **kwargs):
        self._now += timedelta(**kwargs)

    def set(self, dt: datetime):
        self._now = dt


class SignalApplication:
    def __init__(self, data_dir: str = "data/signals",
                 clock: Clock | None = None, shard_size: int = 50,
                 auto_worker: bool = False, poll_seconds: float = 0.2,
                 crash_hook=None):
        self.clock = clock or Clock()
        self.db_path = f"{data_dir}/signal.db"
        self.data_dir = data_dir
        initialize(self.db_path)
        self._lock = threading.RLock()
        self.catalogs = CatalogService(self.db_path, self.clock)
        self.definitions = DefinitionService(self.db_path, self.clock)
        self.sources = SourceService(self.db_path, self.clock)
        self.runner = SignalRunner(
            self.db_path, self.clock, shared_lock=self._lock,
            shard_size=shard_size, crash_hook=crash_hook)
        self.signals = SignalService(self.db_path, self.clock)
        self.results = ResultService(self.db_path)
        self.poll_seconds = poll_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        if auto_worker:
            self._thread = threading.Thread(
                target=self._worker_loop, name="signal-worker", daemon=True)
            self._thread.start()

    # ------------------------------------------------------------------
    # 便捷封装
    # ------------------------------------------------------------------
    def submit_and_wait(self, **kwargs) -> dict:
        """提交作业并同步跑到发布（演示/脚本便捷方法），返回作业状态。"""
        job = self.runner.submit(**kwargs)
        if job["status"] != "COMPLETED":
            self.runner.run_to_completion(job["id"])
            job = self.runner.get_job(job["id"])
        return job

    def audit_events(self, *, event_type: str | None = None,
                     actor: str | None = None, limit: int = 500) -> list[dict]:
        conn = connect(self.db_path)
        try:
            return sig_audit.list_events(conn, event_type=event_type,
                                         actor=actor, limit=limit)
        finally:
            conn.close()

    def verify_audit_chain(self) -> dict:
        conn = connect(self.db_path)
        try:
            return sig_audit.verify_chain(conn)
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 后台工作线程：崩溃恢复 + 检查点续跑
    # ------------------------------------------------------------------
    def _worker_loop(self):
        while not self._stop.wait(self.poll_seconds):
            try:
                pending = self.runner.list_jobs(status="PENDING")
                running = self.runner.list_jobs(status="RUNNING")
                failed = self.runner.list_jobs(status="FAILED")
                # 进程中断（RUNNING）或存在可复位失败分片（FAILED）时先恢复；
                # 仍有硬失败分片的作业 recover 后保持 FAILED，不会被误发布。
                if running or failed:
                    self.runner.recover()
                for job in self.runner.list_jobs(status="PENDING"):
                    self.runner.run_to_completion(job["id"])
            except Exception:
                # 后台循环绝不因单个作业错误退出（失败作业等待人工恢复）。
                pass

    def shutdown(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
