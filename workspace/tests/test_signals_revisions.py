"""信号修订链：追加不覆盖、旧处置保留、过期修订号拒绝。"""
import unittest

from pv_service.signals.signals import SignalError
from tests.signals_base import BaseSignalTest


def make_signal_dataset(t):
    for i in range(1, 9):
        t.source(f"r{i}", ["布洛芬"], ["皮疹"], f"2026-09-{i:02d}")
    t.source("g1", ["布洛芬"], ["恶心"], "2026-09-09")
    t.source("g2", ["布洛芬"], ["恶心"], "2026-09-10")
    t.source("g3", ["阿莫西林"], ["皮疹"], "2026-09-11")
    for i, sid in enumerate(["g4", "g5", "g6", "g7", "g8"]):
        t.source(sid, ["阿莫西林"], ["恶心"], f"2026-09-{12+i:02d}")


class TestSignalRevisions(BaseSignalTest):
    def setUp(self):
        super().setUp()
        make_signal_dataset(self)
        self.run1 = self.run_of(self.submit_full(self.W1))
        self.p1 = self.pair(self.run1, "D_IBU", "E_RASH")
        self.key = "MON1|D_IBU|E_RASH"

    def test_new_revision_and_dispose(self):
        self.assertIsNotNone(self.p1["revision_id"])
        r = self.app.signals.dispose(
            self.p1["revision_id"], "CONFIRMED", "analyst-zhao",
            comment="信号成立")
        self.assertEqual(r["decision"], "CONFIRMED")
        chain = self.app.signals.get_signal(self.key)
        self.assertEqual(chain["latest_seq_no"], 1)
        self.assertEqual(chain["latest_triage_status"], "CONFIRMED")

    def test_disposition_append_only(self):
        self.app.signals.dispose(self.p1["revision_id"], "REJECTED", "a1")
        with self.assertRaises(SignalError) as cm:
            self.app.signals.dispose(self.p1["revision_id"], "CONFIRMED", "a2")
        self.assertEqual(cm.exception.code, "ALREADY_DISPOSED")
        chain = self.app.signals.get_signal(self.key)
        self.assertEqual(chain["revisions"][0]["triage_status"], "REJECTED")

    def test_blank_analyst_rejected(self):
        with self.assertRaises(SignalError) as cm:
            self.app.signals.dispose(self.p1["revision_id"], "CONFIRMED", "  ")
        self.assertEqual(cm.exception.code, "BLANK_ANALYST")

    def test_old_conclusion_retained_new_pending(self):
        self.app.signals.dispose(self.p1["revision_id"], "CONFIRMED", "a1")
        # 窗口2：一条随访削弱信号但未消除（a 8->7，仍超阈值）-> COUNT_CHANGE
        self.clock.advance(days=20)
        self.followup("r2", ["布洛芬"], ["恶心"])
        self.source("n1", ["布洛芬"], ["皮疹"], "2026-09-20")
        run2 = self.run_of(self.submit_full(self.W2))
        p2 = self.pair(run2, "D_IBU", "E_RASH")
        self.assertNotEqual(p2["revision_id"], self.p1["revision_id"])
        self.assertTrue(p2["decision"]["is_signal"])
        chain = self.app.signals.get_signal(self.key)
        self.assertEqual(len(chain["revisions"]), 2)
        # 旧修订 CONFIRMED 保留，新修订 PENDING
        self.assertEqual(chain["revisions"][0]["triage_status"], "CONFIRMED")
        self.assertEqual(chain["revisions"][1]["triage_status"], "PENDING_TRIAGE")
        self.assertEqual(chain["revisions"][1]["change_type"], "COUNT_CHANGE")
        # 默认列表只显示最新修订
        latest = self.app.signals.list_signals(
            definition_id=self.DEFINITION_ID)
        by_key = {s["signal_key"]: s for s in latest}
        self.assertEqual(by_key[self.key]["triage_status"], "PENDING_TRIAGE")

    def test_operate_on_stale_revision_rejected(self):
        self.app.signals.dispose(self.p1["revision_id"], "WATCH", "a1")
        self.clock.advance(days=20)
        self.followup("r2", ["布洛芬"], ["恶心"])
        run2 = self.run_of(self.submit_full(self.W2))
        rev2 = self.pair(run2, "D_IBU", "E_RASH")["revision_id"]
        # 直接对旧修订操作 -> 拒绝
        with self.assertRaises(SignalError) as cm:
            self.app.signals.dispose(self.p1["revision_id"], "REJECTED", "a2")
        self.assertEqual(cm.exception.code, "STALE_REVISION")
        self.assertEqual(cm.exception.extra["current_seq_no"], 2)
        # 带过期 expected_seq_no 操作新修订 -> 拒绝
        with self.assertRaises(SignalError) as cm:
            self.app.signals.dispose(rev2, "CONFIRMED", "a2",
                                     expected_seq_no=1)
        self.assertEqual(cm.exception.code, "STALE_REVISION")
        # 正确序号可以处置
        ok = self.app.signals.dispose(rev2, "CONFIRMED", "a2",
                                      expected_seq_no=2)
        self.assertEqual(ok["seq_no"], 2)

    def test_unchanged_pair_reuses_revision(self):
        # 窗口2 没有任何相关变化（只新增一条无关来源到窗口外日期）
        self.clock.advance(days=20)
        self.source("late", ["华法林"], ["出血"], "2026-10-05")  # 窗口外
        run2 = self.run_of(self.submit_full(self.W2))
        p2 = self.pair(run2, "D_IBU", "E_RASH")
        self.assertEqual(p2["revision_id"], self.p1["revision_id"])
        chain = self.app.signals.get_signal(self.key)
        self.assertEqual(len(chain["revisions"]), 1)

    def test_withdrawn_signal_when_a_drops_to_zero(self):
        # 全部布洛芬皮疹随访改为恶心（跨事件组退出 a 格）
        self.app.signals.dispose(self.p1["revision_id"], "CONFIRMED", "a1")
        self.clock.advance(days=20)
        for i in range(1, 9):
            self.followup(f"r{i}", ["布洛芬"], ["恶心"])
        run2 = self.run_of(self.submit_full(self.W2))
        p2 = self.pair(run2, "D_IBU", "E_RASH")
        self.assertEqual(p2["a"], 0)
        self.assertIsNotNone(p2["revision_id"])
        chain = self.app.signals.get_signal(self.key)
        self.assertEqual(chain["revisions"][-1]["change_type"], "WITHDRAWN")
        # 旧确认保留
        self.assertEqual(chain["revisions"][0]["triage_status"], "CONFIRMED")
        self.assertEqual(chain["revisions"][-1]["triage_status"],
                         "PENDING_TRIAGE")


if __name__ == "__main__":
    unittest.main()
