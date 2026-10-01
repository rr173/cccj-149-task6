"""不成比例指标与信号判定的纯函数测试。"""
import math
import unittest

from pv_service.signals.metrics import (chi_square, compute_metrics, decide,
                                        prr, ror, round6)


class TestMetrics(unittest.TestCase):
    def test_fourfold_known_values(self):
        # a=4,b=1,c=1,d=4: PRR=4, ROR=16
        self.assertAlmostEqual(prr(4, 1, 1, 4), 4.0)
        self.assertAlmostEqual(ror(4, 1, 1, 4), 16.0)

    def test_prr_ror_zero_cells(self):
        self.assertIsNone(prr(0, 0, 1, 4))      # 暴露组无数据
        self.assertIsNone(prr(1, 2, 0, 0))      # 参考组无数据
        self.assertIsNone(prr(1, 2, 0, 3))      # 参考率为 0
        self.assertIsNone(ror(0, 0, 1, 4))
        self.assertIsNone(ror(4, 0, 1, 4))      # b=0 不可定义
        self.assertIsNone(ror(4, 1, 0, 4))      # c=0 不可定义

    def test_chi_square(self):
        x = chi_square(4, 1, 1, 4)
        # N=10; 10*(16-1)^2/(5*5*5*5)=2250/625=3.6
        self.assertAlmostEqual(x, 3.6)
        self.assertIsNone(chi_square(0, 0, 0, 0))
        self.assertIsNone(chi_square(3, 0, 0, 0))

    def test_ci_brackets_estimate(self):
        m = compute_metrics(30, 20, 5, 100)
        self.assertLess(m["prr_ci95_lower"], m["prr"])
        self.assertGreater(m["prr_ci95_upper"], m["prr"])
        self.assertLess(m["ror_ci95_lower"], m["ror"])
        self.assertGreater(m["ror_ci95_upper"], m["ror"])

    def test_ci_zero_cell(self):
        m = compute_metrics(4, 1, 0, 4)
        self.assertIsNone(m["ror"])
        self.assertIsNone(m["ror_ci95_lower"])
        self.assertIsNone(m["prr_ci95_lower"])   # c=0 -> PRR CI 不可计算

    def test_round6_precision(self):
        self.assertEqual(round6(math.pi), 3.141593)
        self.assertIsNone(round6(None))

    # ------------------------------------------------------------------
    # 判定
    # ------------------------------------------------------------------
    def _strong(self):
        return compute_metrics(8, 2, 1, 40)

    def test_decide_signal_all_thresholds(self):
        counts = self._strong()
        r = decide(counts, {"min_cases": 2, "min_prr": 2.0, "min_ror": 2.0,
                           "min_chi_square": 3.841})
        self.assertEqual(r["decision"], "SIGNAL")
        self.assertTrue(r["is_signal"])
        self.assertEqual(r["failed_checks"], [])

    def test_decide_min_cases_gate(self):
        counts = compute_metrics(1, 0, 0, 100)
        r = decide(counts, {"min_cases": 3})
        self.assertEqual(r["decision"], "NO_SIGNAL")
        self.assertEqual(r["failed_checks"][0]["check"], "min_cases")

    def test_decide_uncomputable_metric_fails_gate(self):
        counts = compute_metrics(4, 1, 0, 4)
        r = decide(counts, {"min_ror": 2.0})
        self.assertEqual(r["decision"], "NO_SIGNAL")
        self.assertEqual(r["failed_checks"][0]["check"], "min_ror")
        self.assertIsNone(r["failed_checks"][0]["actual"])

    def test_decide_null_threshold_skipped(self):
        counts = compute_metrics(1, 0, 0, 1)
        r = decide(counts, {"min_cases": None, "min_prr": None})
        self.assertEqual(r["decision"], "SIGNAL")  # 无门槛=全部通过

    def test_decide_ci_lower_gate(self):
        counts = self._strong()
        r_pass = decide(counts, {"min_prr_ci_lower": 1})
        self.assertEqual(r_pass["decision"], "SIGNAL")
        r_fail = decide(counts, {"min_ror_ci_lower": 100})
        self.assertEqual(r_fail["decision"], "NO_SIGNAL")
        self.assertEqual(r_fail["failed_checks"][0]["check"],
                         "min_ror_ci_lower")


if __name__ == "__main__":
    unittest.main()
