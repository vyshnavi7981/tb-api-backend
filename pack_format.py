from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Set



DEFAULT_INT_KEYS: Set[str] = {
    "v", "ts", "fi", "door", "home_floor",
    # add more device-int fields here if the firmware includes them as ints
}

DEFAULT_FLOAT_KEYS: Set[str] = {
    # Heights & sensors (raw)
    "h", "laser_val", "height_raw",
    "accel_x_val", "accel_y_val", "accel_z_val",
    "gyro_x_val",  "gyro_y_val",  "gyro_z_val",
    "mpu_temp_val", "humidity_val", "mic_val",
    # Sometimes firmware sends already-processed names:
    "x_vibe", "y_vibe", "z_vibe",
    "x_jerk", "y_jerk", "z_jerk",
    "temperature", "humidity", "sound_level",
    # Any other continuous numeric fields you might include:
    "vel",
}

__all__ = [
    "parse_pack_raw",
    "ts_seconds",
    "ts_millis",
    "door_to_bit",
    "get_int",
    "get_float",
    "DEFAULT_INT_KEYS",
    "DEFAULT_FLOAT_KEYS",
]


# --- Internal helpers ---------------------------------------------------------

def _to_int(v: str) -> Optional[int]:
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _to_float(v: str) -> Optional[float]:
    try:
        f = float(v)
        # reject NaN/inf by simple check
        if f != f or f in (float("inf"), float("-inf")):
            return None
        return f
    except (ValueError, TypeError):
        return None


def _coerce_value(key: str, val: str,
                  int_keys: Set[str],
                  float_keys: Set[str]) -> Any:
    if val == "":
        return None
    if key in int_keys:
        n = _to_int(val)
        return n if n is not None else val
    if key in float_keys:
        f = _to_float(val)
        return f if f is not None else val
    return val


# --- Public API ---------------------------------------------------------------

def parse_pack_raw(
    s: str,
    *,
    int_keys: Optional[Iterable[str]] = None,
    float_keys: Optional[Iterable[str]] = None,
    lowercase_keys: bool = False,
) -> Dict[str, Any]:
    """
    Parse a 'pack_raw' string into a dict with best-effort typing.

    Args:
        s: The packed string "k=v|k=v|...".
        int_keys: Optional override/extension of keys to force int coercion.
        float_keys: Optional override/extension of keys to force float coercion.
        lowercase_keys: If True, lowercases keys before inserting into dict.

    Returns:
        Dict[str, Any] where numbers are coerced for known keys, unknown keys are strings,
        and empty values become None.
    """
    if not s:
        return {}

    ik: Set[str] = set(DEFAULT_INT_KEYS)
    fk: Set[str] = set(DEFAULT_FLOAT_KEYS)
    if int_keys:
        ik |= set(int_keys)
    if float_keys:
        fk |= set(float_keys)

    out: Dict[str, Any] = {}
    # Fast path split; tolerant to malformed segments
    for pair in s.split("|"):
        if not pair:
            continue
        # Only split on the first '=' to allow '=' inside values (rare)
        eq = pair.find("=")
        if eq < 0:
            continue
        k = pair[:eq].strip()
        v = pair[eq + 1:].strip()
        if not k:
            continue
        if lowercase_keys:
            k = k.lower()
        out[k] = _coerce_value(k, v, ik, fk)
    return out


def ts_seconds(parsed: Dict[str, Any], default: Optional[int] = None) -> Optional[int]:
    """
    Get epoch seconds from parsed dict. Returns `default` if not present or not int-like.
    """
    v = parsed.get("ts")
    if isinstance(v, int):
        return v
    # strings occasionally slip through if the source was malformed
    if isinstance(v, str):
        iv = _to_int(v)
        if iv is not None:
            return iv
    return default


def ts_millis(parsed: Dict[str, Any], default: Optional[int] = None) -> Optional[int]:
    """
    Get epoch milliseconds from parsed dict. If 'ts' is epoch seconds, convert to ms.
    """
    sec = ts_seconds(parsed)
    if sec is None:
        return default
    try:
        return int(sec * 1000)
    except Exception:
        return default


def door_to_bit(val: Any) -> Optional[int]:
    """
    Map various door representations to 1/0:
        - "OPEN" -> 1
        - "CLOSED" / "CLOSE" -> 0
        - numeric truthiness: nonzero -> 1, zero -> 0
        - anything else -> None
    """
    if isinstance(val, str):
        d = val.strip().upper()
        if d == "OPEN":
            return 1
        if d in ("CLOSED", "CLOSE"):
            return 0
        # allow "1"/"0" strings
        iv = _to_int(d)
        if iv is not None:
            return 1 if iv != 0 else 0
        return None
    if isinstance(val, (int, float)):
        try:
            return 1 if int(val) != 0 else 0
        except Exception:
            return None
    if isinstance(val, bool):
        return 1 if val else 0
    return None


def get_int(parsed: Dict[str, Any], key: str, default: Optional[int] = None) -> Optional[int]:
    """
    Convenience getter that tries to coerce to int from parsed dict values.
    """
    v = parsed.get(key)
    if isinstance(v, int):
        return v
    if isinstance(v, (float,)):
        try:
            # only accept if it's actually integral like 3.0
            i = int(v)
            return i if float(i) == v else default
        except Exception:
            return default
    if isinstance(v, str):
        iv = _to_int(v)
        return iv if iv is not None else default
    return default


def get_float(parsed: Dict[str, Any], key: str, default: Optional[float] = None) -> Optional[float]:
    """
    Convenience getter that tries to coerce to float from parsed dict values.
    """
    v = parsed.get(key)
    if isinstance(v, (int, float)):
        try:
            f = float(v)
            # reject NaN/inf
            if f != f or f in (float("inf"), float("-inf")):
                return default
            return f
        except Exception:
            return default
    if isinstance(v, str):
        fv = _to_float(v)
        return fv if fv is not None else default
    return default
