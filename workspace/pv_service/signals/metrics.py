"""不成比例分析指标（纯函数，无副作用，可复现）。

四格表（按 *来源* 去重后的计数；见 ``build.py``）：

========== ========================= =========================
           目标事件组 E              其他事件
目标药品 D  a                         b
其他药品    c                         d
========== ========================= =========================

- PRR = (a/(a+b)) / (c/(c+d))，EBGM 之外最常用的频率比；
- ROR = (a/b) / (c/d) = ad/(bc)，报告比值比；
- 卡方（无 Yates 校正，单自由度）= N(ad-bc)^2 / ((a+b)(c+d)(a+c)(b+d))。

指标统一保留 6 位小数（``round6``）——既保证人工可读，也保证跨平台浮点
差异不会污染输出哈希。零分母时指标为 ``None``（绝不编造数值），95% 置信
区间同样处理。

信号判定（``decide``）逐项检查分析员在监测定义里配置的阈值，任一门槛
关闭（None）即跳过该项，只有 *全部已配置的门槛* 都满足才 SIGNAL；
未达标的对给出具体未过门槛（``failed``），便于复核。
"""
from __future__ import annotations

import math
from typing import Any, Optional

# 指标落库/哈希统一精度
DIGITS = 6


def round6(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return round(float(x), DIGITS)


def prr(a: int, b: int, c: int, d: int) -> Optional[float]:
    """PRR = (a/(a+b)) / (c/(c+d))。参考组或暴露组无数据时 None。"""
    denom_exp = a + b
    denom_ref = c + d
    if denom_exp == 0 or denom_ref == 0:
        return None
    exposed_rate = a / denom_exp
    reference_rate = c / denom_ref
    if reference_rate == 0:
        return None
    return exposed_rate / reference_rate


def ror(a: int, b: int, c: int, d: int) -> Optional[float]:
    """ROR = ad/(bc)。b 或 c 为 0 时不可定义（None）。"""
    if b == 0 or c == 0:
        return None
    return (a * d) / (b * c)


def chi_square(a: int, b: int, c: int, d: int) -> Optional[float]:
    """无 Yates 校正的 Pearson 卡方（df=1）。

    期望频数为零的格子会使分母为 0（无信息可比较），返回 None。
    """
    n = a + b + c + d
    r1, r2 = a + b, c + d
    c1, c2 = a + c, b + d
    if n == 0 or r1 == 0 or r2 == 0 or c1 == 0 or c2 == 0:
        return None
    return n * (a * d - b * c) ** 2 / (r1 * r2 * c1 * c2)


def _ci_log_ratio(est: Optional[float], a: int, b: int, c: int, d: int,
                  kind: str) -> tuple[Optional[float], Optional[float]]:
    """对数尺度 Wald 95% 置信区间。

    PRR: SE = sqrt(1/a - 1/(a+b) + 1/c - 1/(c+d))（EVANS 常用式）；
    ROR: SE = sqrt(1/a + 1/b + 1/c + 1/d)。
    任一相关格为 0（估计本身不可定义或方差无限）时返回 (None, None)。
    """
    if est is None or est <= 0:
        return None, None
    try:
        if kind == "PRR":
            if a == 0 or c == 0:
                return None, None
            var = 1 / a - 1 / (a + b) + 1 / c - 1 / (c + d)
        else:  # ROR
            if 0 in (a, b, c, d):
                return None, None
            var = 1 / a + 1 / b + 1 / c + 1 / d
        if var <= 0:
            return None, None
        se = math.sqrt(var)
    except ZeroDivisionError:
        return None, None
    log_est = math.log(est)
    return round6(math.exp(log_est - 1.96 * se)), round6(math.exp(log_est + 1.96 * se))


def compute_metrics(a: int, b: int, c: int, d: int) -> dict[str, Any]:
    """四格 -> 全部指标（None 安全、6 位小数）。"""
    p = prr(a, b, c, d)
    r = ror(a, b, c, d)
    p_lo, p_hi = _ci_log_ratio(p, a, b, c, d, "PRR")
    r_lo, r_hi = _ci_log_ratio(r, a, b, c, d, "ROR")
    return {
        "a": int(a), "b": int(b), "c": int(c), "d": int(d),
        "n": int(a + b + c + d),
        "prr": round6(p),
        "prr_ci95_lower": p_lo,           # Evans 式 log-Wald 双侧 95%
        "prr_ci95_upper": p_hi,
        "ror": round6(r),
        "ror_ci95_lower": r_lo,
        "ror_ci95_upper": r_hi,
        "chi_square": round6(chi_square(a, b, c, d)),
    }


def decide(counts: dict[str, Any], thresholds: dict[str, Any]) -> dict[str, Any]:
    """按监测定义阈值给出信号判定。

    thresholds 支持（均可为 null/缺省 = 不检查该项）：

    - ``min_cases``：a 格最小样本量（药品组+事件组病例数，含来源去重后）；
    - ``min_prr``：PRR 下限（不可计算视为不满足）；
    - ``min_prr_ci_lower``：PRR 95% CI 下界（EB05 风格，常用 >1）；
    - ``min_ror``：ROR 下限；
    - ``min_ror_ci_lower``：ROR 95% CI 下界 >1；
    - ``min_chi_square``：卡方门槛（常用 3.841）。
    """
    failed: list[dict[str, Any]] = []

    def fail(check: str, expected: Any, actual: Any):
        failed.append({"check": check, "expected": expected, "actual": actual})

    checks = (
        ("min_cases", lambda: counts["a"], lambda exp, act: act >= exp),
        ("min_prr", lambda: counts.get("prr"), lambda exp, act: act is not None and act >= exp),
        ("min_prr_ci_lower", lambda: counts.get("prr_ci95_lower"),
         lambda exp, act: act is not None and act > exp),
        ("min_ror", lambda: counts.get("ror"), lambda exp, act: act is not None and act >= exp),
        ("min_ror_ci_lower", lambda: counts.get("ror_ci95_lower"),
         lambda exp, act: act is not None and act > exp),
        ("min_chi_square", lambda: counts.get("chi_square"),
         lambda exp, act: act is not None and act >= exp),
    )
    for key, getter, ok in checks:
        if key not in thresholds or thresholds[key] is None:
            continue
        expected = thresholds[key]
        actual = getter()
        if not ok(expected, actual):
            fail(key, expected, actual)
    is_signal = not failed
    return {
        "decision": "SIGNAL" if is_signal else "NO_SIGNAL",
        "is_signal": is_signal,
        "applied_thresholds": {
            k: thresholds.get(k) for k, _, _ in checks if thresholds.get(k) is not None},
        "failed_checks": failed,
    }
