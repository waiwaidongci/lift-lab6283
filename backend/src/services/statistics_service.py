"""Welch comparison and pooled CUPED, using exact, mergeable sufficient statistics.

Every observation is converted to an exact integer scaled by 2**1074 (every finite
IEEE-754 double is an integer multiple of 2**-1074).  Per-arm sums, sums of squares
and cross-products are therefore exact Python integers.  Integer addition is
associative and commutative, so merging shards is exact: the result does not depend
on batch size or batch order, and a very large common revenue base cannot hide a
small treatment difference through floating-point cancellation.

Only the final moments are converted to float/Fraction, once, in :func:`finalize`.
"""
from fractions import Fraction
from math import isfinite, sqrt

# 2**-1074 is the smallest positive IEEE-754 double, so every finite double is an
# integer multiple of it.
SCALE_BITS = 1074
SCALE = 1 << SCALE_BITS
SCALE_SQUARED = 1 << (2 * SCALE_BITS)

ARMS = ("control", "treatment")
FIELDS = ("n", "sx", "sy", "sxx", "syy", "sxy")


def _empty_group():
    return {field: 0 for field in FIELDS}


def empty_state():
    return {"total": 0, "control": _empty_group(), "treatment": _empty_group()}


def _scaled(value):
    """Return value * 2**1074 as an exact integer without loss."""
    number = float(value)
    if not isfinite(number):
        raise ValueError("观测含非有限值")
    numerator, denominator = number.as_integer_ratio()
    # as_integer_ratio returns a reduced fraction whose denominator is a power of
    # two.  The shift is always non-negative for finite doubles.
    return numerator << (SCALE_BITS - (denominator.bit_length() - 1))


class Accumulator:
    """Exact per-arm sufficient statistics for one shard or a whole dataset."""

    def __init__(self, state=None):
        self.state = empty_state() if state is None else _copy_state(state)

    def add(self, arm, pre_value, value):
        if arm not in ARMS:
            raise ValueError("存在未知实验分组")
        x = _scaled(pre_value)
        y = _scaled(value)
        group = self.state[arm]
        group["n"] += 1
        group["sx"] += x
        group["sy"] += y
        group["sxx"] += x * x
        group["syy"] += y * y
        group["sxy"] += x * y
        self.state["total"] += 1

    def merge(self, other):
        source = other.state if isinstance(other, Accumulator) else other
        for arm in ARMS:
            target, incoming = self.state[arm], source[arm]
            for field in FIELDS:
                target[field] += int(incoming[field])
        self.state["total"] += int(source.get("total", 0))
        return self

    def to_dict(self):
        return _copy_state(self.state)


def _copy_state(state):
    state = state or {}
    return {
        "total": int(state.get("total", 0)),
        **{
            arm: {field: int((state.get(arm) or {}).get(field, 0)) for field in FIELDS}
            for arm in ARMS
        },
    }


def merge_states(states):
    """Merge encoded shard states in any order, exactly."""
    accumulator = Accumulator()
    for state in states:
        accumulator.merge(state)
    return accumulator


def encode_state(state):
    """JSON-safe encoding: big integers become decimal strings.

    Checkpoints must round-trip through PostgreSQL ``json`` without any numeric
    coercion, and the scaled integers can exceed float64 precision.
    """
    state = _copy_state(state)
    return {
        "total": str(state["total"]),
        **{
            arm: {field: str(state[arm][field]) for field in FIELDS}
            for arm in ARMS
        },
    }


def decode_state(encoded):
    """Inverse of :func:`encode_state`; tolerant of legacy plain integers."""
    encoded = encoded or empty_state()
    return {
        "total": int(encoded.get("total", 0)),
        **{
            arm: {field: int(encoded.get(arm, {}).get(field, 0)) for field in FIELDS}
            for arm in ARMS
        },
    }


def _mean(group):
    return Fraction(group["sy"], group["n"] * SCALE)


def _variance(group):
    n = group["n"]
    numerator = n * group["syy"] - group["sy"] * group["sy"]
    return Fraction(numerator, n * (n - 1) * SCALE_SQUARED)


def _to_float(value):
    """Convert an exact Fraction to float, saturating instead of overflowing."""
    try:
        return float(value)
    except OverflowError:
        return float("inf") if value > 0 else float("-inf")


def _welch(n_control, mean_control, var_control, n_treatment, mean_treatment, var_treatment, alpha):
    from scipy.stats import t  # lazy: keep the worker's idle memory small

    if min(n_control, n_treatment) < 3:
        raise ValueError("每组至少需要 3 个独立样本")
    va, vb = _to_float(var_control), _to_float(var_treatment)
    se2 = va / n_control + vb / n_treatment
    if se2 <= 1e-14:
        raise ValueError("组内方差为零，不能执行 Welch 检验")
    se = sqrt(se2)
    mean_a, mean_b = _to_float(mean_control), _to_float(mean_treatment)
    # Subtract the exact means before rounding: with a huge common revenue base a
    # float64 subtraction of the two means would erase a small treatment effect.
    difference = _to_float(mean_treatment - mean_control)
    df = se2**2 / ((va / n_control) ** 2 / (n_control - 1) + (vb / n_treatment) ** 2 / (n_treatment - 1))
    critical = float(t.ppf(1 - alpha / 2, df))
    return {
        "control_mean": mean_a,
        "treatment_mean": mean_b,
        "difference": difference,
        "standard_error": se,
        "degrees_of_freedom": df,
        "p_value": float(2 * t.sf(abs(difference / se), df)),
        "ci_low": difference - critical * se,
        "ci_high": difference + critical * se,
        "n_control": n_control,
        "n_treatment": n_treatment,
    }


def _adjusted_moments(group, theta, pooled_mean_x):
    """Exact first two moments of Y - theta * (X - pooled_mean_x) for one arm."""
    n = group["n"]
    sum_z = Fraction(group["sy"], SCALE) - theta * (Fraction(group["sx"], SCALE) - n * pooled_mean_x)
    mean_z = sum_z / n
    sum_z_squared = (
        Fraction(group["syy"], SCALE_SQUARED)
        - 2 * theta * Fraction(group["sxy"], SCALE_SQUARED)
        + theta * theta * Fraction(group["sxx"], SCALE_SQUARED)
        + 2 * theta * pooled_mean_x * Fraction(group["sy"], SCALE)
        - 2 * theta * theta * pooled_mean_x * Fraction(group["sx"], SCALE)
        + n * theta * theta * pooled_mean_x * pooled_mean_x
    )
    variance_z = (sum_z_squared - n * mean_z * mean_z) / (n - 1)
    return mean_z, variance_z


def finalize(state, alpha=.05, use_cuped=True):
    """Turn exact merged state into the same result fields as before."""
    if not 0 < alpha < .5:
        raise ValueError("alpha 必须位于 (0,0.5)")
    state = _copy_state(state)
    if state["total"] == 0:
        raise ValueError("数据集为空")
    control, treatment = state["control"], state["treatment"]
    if min(control["n"], treatment["n"]) < 3:
        raise ValueError("每组至少需要 3 个独立样本")

    raw = _welch(
        control["n"], _mean(control), _variance(control),
        treatment["n"], _mean(treatment), _variance(treatment), alpha,
    )

    theta = Fraction(0)
    adjusted = None
    reduction = 0.
    if use_cuped:
        total_x = control["sx"] + treatment["sx"]
        total_y = control["sy"] + treatment["sy"]
        total_xy = control["sxy"] + treatment["sxy"]
        total_xx = control["sxx"] + treatment["sxx"]
        covariance_numerator = state["total"] * total_xy - total_x * total_y
        variance_numerator = state["total"] * total_xx - total_x * total_x
        pooled_mean_x = Fraction(total_x, state["total"] * SCALE)
        if variance_numerator == 0 or float(Fraction(variance_numerator, state["total"] * (state["total"] - 1) * SCALE_SQUARED)) <= 1e-14:
            raise ValueError("前置指标为常量，CUPED 无法计算")
        theta = Fraction(covariance_numerator, variance_numerator)
        mean_a, var_a = _adjusted_moments(control, theta, pooled_mean_x)
        mean_b, var_b = _adjusted_moments(treatment, theta, pooled_mean_x)
        adjusted = _welch(control["n"], mean_a, var_a, treatment["n"], mean_b, var_b, alpha)
        reduction = 1 - adjusted["standard_error"] ** 2 / raw["standard_error"] ** 2

    chosen = adjusted or raw
    return {
        "raw": raw,
        "cuped": adjusted,
        "theta": float(theta),
        "variance_reduction": reduction,
        "alpha": alpha,
        "significant": chosen["p_value"] < alpha,
        "method": "Welch + pooled CUPED" if use_cuped else "Welch",
        "interpretation": "区间完全高于零，可供放量会议审议" if chosen["ci_low"] > 0 else "证据不足或效果为负，建议保留观察",
        "assumptions": "用户级独立随机分桶；前置变量须在分桶前取得；单一预注册指标，未做多重比较校正",
        "sample_size": state["total"],
    }


def analyze(observations, alpha=.05, use_cuped=True):
    """Convenience entry point for in-memory observation objects.

    The worker never calls this with a whole dataset; it uses chunked accumulation.
    """
    accumulator = Accumulator()
    for observation in observations:
        accumulator.add(observation.arm, observation.pre_value, observation.value)
    return finalize(accumulator.state, alpha, use_cuped)
