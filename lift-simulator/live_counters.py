import os
import json
import time
import math
import logging
from typing import Dict, Optional, Tuple

import requests
from thingsboard_auth import get_admin_jwt

logger = logging.getLogger("live_counters")
logging.basicConfig(level=logging.INFO)

TB_BASE_URL = os.getenv("TB_BASE_URL", "https://thingsboard.cloud").rstrip("/")

# ---- Behavior knobs ----
LC_ENABLED = os.getenv("LC_ENABLED", "true").lower() in ("1", "true", "yes")
LC_TZ = os.getenv("LC_TZ", "UTC")  # e.g., "+05:30" or "UTC"
LC_MOVEMENT_THRESHOLD_MM = float(os.getenv("LC_MOVEMENT_THRESHOLD_MM", "50"))
LC_KEY_TTL_HOURS = int(os.getenv("LC_REDIS_KEY_TTL_HOURS", "48"))  # kept name for compatibility (used for info only)
LC_DEBUG = os.getenv("LC_DEBUG", "0") in ("1", "true", "TRUE", "yes", "on")

# ---- In-memory storage (no Redis) ----
_inmem: Dict[str, Dict[str, int]] = {}      # daily per-device counters (hash-like): key -> {field->value}
_state_inmem: Dict[str, Dict[str, str]] = {}  # last sample state per device

def _dbg(msg: str, *args):
    if LC_DEBUG:
        logger.info("[LC_DEBUG] " + msg, *args)

def _local_date_str(ts_ms: int) -> str:
    """
    Convert epoch ms to local date string YYYY-MM-DD using LC_TZ.
    If LC_TZ is like "+05:30" or "-04:00", use that fixed offset.
    Otherwise treat as UTC (keeps implementation light).
    """
    tz = LC_TZ.strip()
    if tz.startswith(("+", "-")) and len(tz) >= 3 and ":" in tz:
        sign = 1 if tz[0] == "+" else -1
        try:
            hh, mm = tz[1:].split(":", 1)
            offset_sec = sign * (int(hh) * 3600 + int(mm) * 60)
            sec = (ts_ms // 1000) + offset_sec
            return time.strftime("%Y-%m-%d", time.gmtime(sec))
        except Exception:
            pass
    return time.strftime("%Y-%m-%d", time.gmtime(ts_ms / 1000.0))

def _to_float(x) -> float:
    try:
        f = float(x)
        if f != f or f in (float("inf"), float("-inf")):
            return float("nan")
        return f
    except Exception:
        return float("nan")

def _parse_pack_out(v: str) -> Tuple[Optional[str], float, Optional[bool]]:
    """
    Extract (floor_label, height_mm, door_open) from pack_out (JSON or k=v|k=v).
    Accepts both your compact keys and enriched compat keys:
      - floor: 'floor_label' or 'fl'
      - height: 'height' or 'h'
      - door: 'door_open' or 'door' (0/1) or 'door_val' ('OPEN'/'CLOSED')
    """
    floor_label, height_mm, door_open = None, float("nan"), None
    if not v:
        return floor_label, height_mm, door_open

    # Try JSON first
    try:
        j = json.loads(v)
        if isinstance(j, dict):
            floor_label = (j.get("floor_label") or j.get("fl"))
            h_raw = j.get("height")
            if h_raw is None:
                h_raw = j.get("h")
            height_mm = _to_float(h_raw) if h_raw is not None else float("nan")

            if "door_open" in j:
                door_open = bool(j["door_open"])
            elif "door" in j:
                try:
                    door_open = bool(int(j["door"]))
                except Exception:
                    door_open = None
            elif "door_val" in j:
                door_open = str(j["door_val"]).strip().upper() == "OPEN"
            return floor_label, height_mm, door_open
    except Exception:
        pass

    # Fallback: parse k=v|k=v
    parts = {}
    for p in v.split("|"):
        if "=" in p:
            k, vv = p.split("=", 1)
            parts[k] = vv

    floor_label = parts.get("floor_label") or parts.get("fl")
    # height
    h_raw = parts.get("height")
    if h_raw is None:
        h_raw = parts.get("h")
    height_mm = _to_float(h_raw) if h_raw is not None else float("nan")
    # door
    if "door_open" in parts:
        try:
            door_open = bool(int(parts["door_open"]))
        except Exception:
            door_open = parts["door_open"].strip().lower() in ("true", "open", "1")
    elif "door" in parts:
        try:
            door_open = bool(int(parts["door"]))
        except Exception:
            door_open = None
    elif "door_val" in parts:
        door_open = parts["door_val"].strip().upper() == "OPEN"

    return floor_label, height_mm, door_open

def _movement(prev_h: float, h: float, thr: float) -> bool:
    if math.isnan(prev_h) or math.isnan(h):
        return False
    return abs(h - prev_h) > thr

# ---------- State & counters storage helpers ----------

def _state_key(device_id: str) -> str:
    return f"lc:state:{device_id}"

def _door_key(date_str: str, device_id: str) -> str:
    return f"lc:{date_str}:{device_id}:door"  # hash: floor -> opens count

def _idle_key(date_str: str, device_id: str) -> str:
    return f"lc:{date_str}:{device_id}:idle_ms"  # hash: floor -> milliseconds

def _hinc(store_key: str, field: str, delta: int) -> None:
    h = _inmem.setdefault(store_key, {})
    h[field] = int(h.get(field, 0)) + int(delta)

def _hgetall(store_key: str) -> Dict[str, int]:
    return {k: int(v) for k, v in _inmem.get(store_key, {}).items()}

def _state_get(device_id: str) -> Dict[str, str]:
    return _state_inmem.get(device_id, {}).copy()

def _state_set(device_id: str, d: Dict[str, str]) -> None:
    _state_inmem[device_id] = d.copy()

# ---------- Public API ----------

def process_pack_out_sample(device_id: str,
                            device_name: str,
                            ts_ms: int,
                            pack_out_str: str) -> None:
    """
    Incrementally update counters for this sample.
    This version treats "idle" as "not moving" regardless of door state.
    Door-open count still uses CLOSED->OPEN rising edge.
    """
    if not LC_ENABLED:
        return

    fl, h, dopen = _parse_pack_out(pack_out_str)
    if fl is None and math.isnan(h) and dopen is None:
        return  # nothing useful

    state = _state_get(device_id)
    last_ts = int(state.get("ts", "0") or "0")
    last_floor = state.get("floor")
    try:
        last_h = float(state.get("h")) if "h" in state else float("nan")
    except Exception:
        last_h = float("nan")
    last_door = state.get("door")       # "1" / "0" / ""
    last_door_bool = None if last_door is None or last_door == "" else (last_door == "1")

    # dedupe / ordering guard
    if ts_ms <= last_ts:
        return

    # Compute date bucket
    date_str = _local_date_str(ts_ms)
    floor_for_bucket = (fl or last_floor or "UNKNOWN")

    # Rising edge detection for door-open counter
    # Previous CLOSED (0/False) -> current OPEN (1/True) increments by 1
    if last_door_bool in (False, 0) and dopen in (True, 1):
        _hinc(_door_key(date_str, device_id), floor_for_bucket, 1)
        _dbg("Door OPEN edge on %s floor=%s", device_name, floor_for_bucket)

    # --- Idle accumulation (movement-only) ---
    # If there was NO movement between last sample and this sample, accrue dt to idle.
    # Door state is ignored by request.
    if not _movement(last_h, h, LC_MOVEMENT_THRESHOLD_MM):
        dt = ts_ms - last_ts
        if dt > 0 and last_ts > 0:
            _hinc(_idle_key(date_str, device_id), floor_for_bucket, dt)
            _dbg("Idle +%dms on %s floor=%s (h_prev=%.1f h_now=%.1f thr=%.1f)",
                 dt, device_name, floor_for_bucket, last_h, h, LC_MOVEMENT_THRESHOLD_MM)

    # Persist new state
    _state_set(device_id, {
        "ts": str(ts_ms),
        "floor": floor_for_bucket,
        "h": "nan" if math.isnan(h) else str(h),
        # Keep door state for door-open edge detection
        "door": "1" if (dopen in (True, 1)) else ("0" if dopen in (False, 0) else (last_door or ""))
    })

def flush_day_to_tb(date_str: str) -> int:
    """
    Push aggregated counters for the given date to ThingsBoard for all devices seen that day.
    Returns number of devices flushed.
    Note: with in-memory storage, device counters vanish on process restart; this is for test/dev.
    """
    # Discover candidates from in-memory keys
    candidates = set()
    for key in list(_inmem.keys()):
        if key.startswith(f"lc:{date_str}:") and (key.endswith(":door") or key.endswith(":idle_ms")):
            parts = key.split(":")
            if len(parts) >= 4:
                candidates.add(parts[2])

    if not candidates:
        logger.info("[LiveCounters] No devices to flush for %s", date_str)
        return 0

    jwt = get_admin_jwt()
    flushed = 0
    write_ts_ms = int(time.time() * 1000) - 1

    for device_id in candidates:
        door_counts = _hgetall(_door_key(date_str, device_id))
        idle_ms = _hgetall(_idle_key(date_str, device_id))
        idle_sec = {k: int(round(v / 1000.0)) for k, v in idle_ms.items()}

        payload = {
            "daily_floor_door_opens": door_counts,
            "daily_floor_idle_sec": idle_sec,
            "daily_floor_summary": {"date": date_str, "door_opens": door_counts, "idle_sec": idle_sec}
        }
        # Save telemetry
        url = f"{TB_BASE_URL}/api/plugins/telemetry/DEVICE/{device_id}/timeseries/ANY"
        body = {"ts": write_ts_ms, "values": {}}
        for k, v in payload.items():
            if isinstance(v, (dict, list)):
                body["values"][k] = json.dumps(v, separators=(",", ":"))
            else:
                body["values"][k] = v
        r = requests.post(
            url, headers={"X-Authorization": f"Bearer {jwt}", "Content-Type": "application/json"},
            data=json.dumps(body), timeout=45
        )
        if r.status_code >= 400:
            logger.error("[LiveCounters] TB save_ts failed for %s (%s): %s", device_id, r.status_code, r.text)
            continue

        flushed += 1

        # Clear only that day's keys after a flush (so repeated flushes don’t duplicate)
        _inmem.pop(_door_key(date_str, device_id), None)
        _inmem.pop(_idle_key(date_str, device_id), None)

    logger.info("[LiveCounters] Flushed %d device(s) for %s", flushed, date_str)
    return flushed
