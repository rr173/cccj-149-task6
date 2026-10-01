"""分片作业：检查点续跑、任一分片失败不发布、并发同键单作业。"""
import concurrent.futures
import threading
import unittest

from pv_service.signals.runner import RunnerError
from tests.signals_base import BaseSignalTest


def make_data(t, n=12):
    for i in range(1, n + 1):
        t.source(f"s{i:02d}", ["布洛芬"], ["皮疹"], f"2026-09-{i:02d}")


class TestShardedJobs(BaseSignalTest):
    def setUp(self):
        super().setUp(shard_size=4)
        make_data(self)

    def _job(self, window=BaseSignalTest.W1):
        return self.app.runner.submit(
            definition_id=self.DEFINITION_ID,
            window_start=window[0], window_end=window[1], cutoff=window[2],
            term_catalog_id=self.CATALOG_ID)

    def test_shard_checkpoint_progress(self):
        job = self._job()
        self.assertEqual(job["shard_count"], 3)
        self.app.runner.tick(job["id"])
        j = self.app.runner.get_job(job["id"])
        self.assertEqual(j["committed_shards"], 1)
        self.assertIsNone(j["run_id"])  # 未发布
        # 重放已提交分片幂等
        r = self.app.runner.run_shard(job["id"], 0)
        self.assertTrue(r.get("deduped"))
        self.app.runner.run_to_completion(job["id"])
        j = self.app.runner.get_job(job["id"])
        self.assertEqual(j["status"], "COMPLETED")
        self.assertIsNotNone(j["run_id"])

    def test_no_half_published_when_shard_fails(self):
        job = self._job()
        # 让第 2 个分片失败
        def hook(phase, jid, shard):
            if phase == "shard_committed" and shard == 1:
                raise RuntimeError("boom")
        self.app.runner.crash_hook = hook
        self.app.runner.tick(job["id"])  # shard 0 ok
        with self.assertRaises(RuntimeError):
            self.app.runner.tick(job["id"])  # shard 1 fails
        j = self.app.runner.get_job(job["id"])
        self.assertEqual(j["status"], "FAILED")
        self.assertEqual(self.app.results.list_runs(), [])  # 无半成品指标
        # 有失败分片时 tick 拒绝
        self.app.runner.crash_hook = None
        with self.assertRaises(RunnerError) as cm:
            self.app.runner.tick(job["id"])
        self.assertEqual(cm.exception.code, "SHARD_FAILED")
        # 恢复后续跑发布
        self.app.runner.recover()
        self.app.runner.run_to_completion(job["id"])
        run = self.app.results.get_run(
            self.app.runner.get_job(job["id"])["run_id"])
        self.assertEqual(run["total_sources"], 12)
        self.assertEqual(self.pair(run, "D_IBU", "E_RASH")["a"], 12)

    def test_crash_after_commit_skips_done_shards(self):
        job = self._job()
        # 提交 shard 1 之后“进程被杀”（提交后钩子，作业停留 RUNNING）
        killed = threading.Event()

        def hook(phase, jid, shard):
            if phase == "after_shard_commit" and shard == 1 and not killed.is_set():
                killed.set()
                raise KeyboardInterrupt
        self.app.runner.crash_hook = hook
        self.app.runner.tick(job["id"])  # shard 0
        with self.assertRaises(KeyboardInterrupt):
            self.app.runner.tick(job["id"])  # shard 1 提交后被杀
        j = self.app.runner.get_job(job["id"])
        self.assertEqual(j["status"], "RUNNING")
        self.assertEqual(
            [s["status"] for s in j["shards"]],
            ["COMMITTED", "COMMITTED", "PENDING"])
        # 重启：恢复不重做已提交分片，只补 shard 2
        self.app.runner.crash_hook = None
        rec = self.app.runner.recover()
        self.assertEqual(rec["reset_shards"], 0)
        self.app.runner.run_to_completion(job["id"])
        run = self.app.results.get_run(
            self.app.runner.get_job(job["id"])["run_id"])
        self.assertEqual(run["total_sources"], 12)  # 无重复贡献

    def test_recover_idempotent_audit(self):
        job = self._job()
        self.app.runner.tick(job["id"])
        n1 = len(self.app.audit_events())
        self.app.runner.recover()
        self.app.runner.recover()
        events = [e for e in self.app.audit_events()
                  if e["event_type"] == "RUN_RECOVERED"]
        # 无实质复位时第二次恢复不产生新审计帧（同幂等键）
        self.assertEqual(len(events), 1)
        self.assertGreaterEqual(len(self.app.audit_events()), n1)

    def test_concurrent_same_key_single_job(self):
        def submit(_):
            return self._job()
        with concurrent.futures.ThreadPoolExecutor(10) as ex:
            jobs = list(ex.map(submit, range(10)))
        ids = {j["id"] for j in jobs}
        self.assertEqual(len(ids), 1)
        self.assertEqual(sum(1 for j in jobs if j.get("deduped")), 9)

    def test_concurrent_same_key_single_published_run(self):
        def submit_and_run(_):
            job = self._job()
            self.app.runner.run_to_completion(job["id"])
            return self.app.runner.get_job(job["id"])
        with concurrent.futures.ThreadPoolExecutor(6) as ex:
            jobs = list(ex.map(submit_and_run, range(6)))
        self.assertEqual(len({j["run_id"] for j in jobs}), 1)
        runs = self.app.results.list_runs()
        self.assertEqual(len(runs), 1)

    def test_recover_auto_resume(self):
        job = self._job()

        def hook(phase, jid, shard):
            if phase == "shard_committed" and shard == 0:
                raise RuntimeError("x")
        self.app.runner.crash_hook = hook
        with self.assertRaises(RuntimeError):
            self.app.runner.tick(job["id"])
        self.app.runner.crash_hook = None
        # recover 只复位不续跑
        self.assertEqual(self.app.runner.get_job(job["id"])["status"], "FAILED")
        self.app.runner.recover()
        self.assertEqual(self.app.runner.get_job(job["id"])["status"], "PENDING")
        self.app.runner.run_to_completion(job["id"])
        self.assertEqual(self.app.runner.get_job(job["id"])["status"],
                         "COMPLETED")


    def test_auto_worker_resumes_interrupted_job(self):
        from pv_service.signals.service import SignalApplication
        # 新应用（auto_worker）打开同一库：中断作业被自动恢复并续跑发布
        job = self._job()

        def hook(phase, jid, shard):
            if phase == "shard_committed" and shard == 0:
                raise RuntimeError("x")
        self.app.runner.crash_hook = hook
        with self.assertRaises(RuntimeError):
            self.app.runner.tick(job["id"])
        self.app.runner.crash_hook = None
        app2 = SignalApplication(self.dir, clock=self.clock, shard_size=4,
                                 auto_worker=True, poll_seconds=0.02)
        try:
            deadline = 100
            while app2.runner.get_job(job["id"])["status"] != "COMPLETED" and deadline:
                __import__("time").sleep(0.02)
                deadline -= 1
            self.assertEqual(app2.runner.get_job(job["id"])["status"], "COMPLETED")
        finally:
            app2.shutdown()


if __name__ == "__main__":
    unittest.main()
