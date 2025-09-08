# alarm_logic.py
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field, model_validator

from pack_format import parse_pack_raw, ts_millis, door_to_bit, get_float
from thingsboard_auth import get_admin_jwt  # signature: get_admin_jwt(account_id: str, host: str) -> str

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("alarm_logic")

router = APIRouter()

# ---- Accounts ---------------------------------------------------------------
def _load_tb_accounts() -> Dict[str, str]:
    raw = os.getenv("TB_ACCOUNTS", "").strip()
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict) and data:
                return {str(k): str(v) for k, v in data.items()}
        except Exception as e:
            logger.warning("[TB_ACCOUNTS] parse failed: %s", e)
    base = os.getenv("TB_BASE_URL", "https://thingsboard.cloud").strip()
    return {"default": base}

ACCOUNTS = _load_tb_accounts()
logger.info("[INIT] Loaded ThingsBoard accounts: %s", list(ACCOUNTS.keys()))

def _resolve_account(x_account_id: Optional[str]) -> str:
    if x_account_id:
        if x_account_id in ACCOUNTS:
            return x_account_id
        if x_account_id.lower() in ACCOUNTS:
            return x_account_id.lower()
    return next(iter(ACCOUNTS.keys()))

# ---- Thresholds & constants --------------------------------------------------
THRESHOLDS = {
    "humidity": 50.0,
    "temperature": 50.0,
    "x_jerk": 5.0, "y_jerk": 5.0, "z_jerk": 15.0,
    "x_vibe": 5.0, "y_vibe": 5.0, "z_vibe": 15.0,
}
DOOR_OPEN_THRESHOLD_SEC = 15
TOLERANCE_MM = 10.0
BUCKET_HALF_MM = 50.0
MOVEMENT_DEADBAND_MM = 20.0
FLOOR_CACHE_TTL_SEC = 300

# ---- Payload model -----------------------------------------------------------
class AlarmIn(BaseModel):
    deviceName: str = Field(...)
    device_token: Optional[str] = Field(default=None)

    # accept any of these for the packed string
    pack_raw: Optional[str] = Field(default=None)
    pack_out: Optional[str] = Field(default=None)  # NEW: allow pack_out as alias
    raw: Optional[str] = Field(default=None)
    pack: Optional[str] = Field(default=None)
    payload: Optional[Dict[str, Any]] = Field(default=None)

    ts: Optional[int] = Field(default=None, description="epoch ms (optional)")

    @model_validator(mode="after")
    def unify_pack(self):
        # Normalize to pack_raw
        if not self.pack_raw:
            if self.pack_out:
                self.pack_raw = self.pack_out
            elif self.raw:
                self.pack_raw = self.raw
            elif self.pack:
                self.pack_raw = self.pack
            elif isinstance(self.payload, dict):
                self.pack_raw = (
                    self.payload.get("pack_raw")
                    or self.payload.get("pack_out")
                    or self.payload.get("raw")
                    or self.payload.get("pack")
                )
        return self

# ---- Caches/state ------------------------------------------------------------
_device_id_cache: Dict[str, str] = {}
_floor_meta_cache: Dict[str, Dict[str, Any]] = {}
_device_door_state: Dict[str, bool] = {}
_door_open_since: Dict[str, float] = {}
_bucket_counts: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
_movement_state: Dict[str, Dict[str, Any]] = {}

# ---- TB helpers --------------------------------------------------------------
def _admin_headers(account_id: str) -> Dict[str, str]:
    host = ACCOUNTS[account_id]
    jwt = get_admin_jwt(account_id, host)
    return {"Content-Type": "application/json", "X-Authorization": f"Bearer {jwt}"}

def _get_device_id(device_name: str, account_id: str) -> Optional[str]:
    cache_key = f"{account_id}:{device_name}"
    if cache_key in _device_id_cache:
        return _device_id_cache[cache_key]
    host = ACCOUNTS[account_id]
    url = f"{host}/api/tenant/devices?deviceName={device_name}"
    res = requests.get(url, headers=_admin_headers(account_id), timeout=10)
    logger.info(f"[DEVICE_LOOKUP] {device_name} ({account_id}) | Status: {res.status_code}")
    if res.ok:
        try:
            device_id = res.json()["id"]["id"]
            _device_id_cache[cache_key] = device_id
            return device_id
        except Exception as e:
            logger.error(f"[DEVICE_LOOKUP] parse error: {e}")
    else:
        logger.error(f"[DEVICE_LOOKUP] {res.status_code} | {res.text}")
    return None

def _fetch_server_attributes(device_id: str, account_id: str) -> Dict[str, Any]:
    host = ACCOUNTS[account_id]
    url = f"{host}/api/plugins/telemetry/DEVICE/{device_id}/values/attributes/SERVER_SCOPE"
    r = requests.get(url, headers=_admin_headers(account_id), timeout=10)
    r.raise_for_status()
    out = {}
    for item in r.json():
        out[item["key"]] = item.get("value")
    return out

def _get_floor_meta(device_name: str, account_id: str) -> Tuple[list, list, Optional[int]]:
    key = f"{account_id}:{device_name}"
    now = time.time()
    cached = _floor_meta_cache.get(key)
    if cached and now - cached.get("ts", 0) < FLOOR_CACHE_TTL_SEC:
        return cached["boundaries"], cached["labels"], cached.get("home_floor")

    dev_id = _get_device_id(device_name, account_id)
    boundaries, labels, home_floor = [], [], None
    if dev_id:
        try:
            attrs = _fetch_server_attributes(dev_id, account_id)
            fb_raw = attrs.get("floor_boundaries")
            fl_raw = attrs.get("floor_labels")
            hf_raw = attrs.get("home_floor")
            if isinstance(fb_raw, str):
                boundaries = [int(x.strip()) for x in fb_raw.split(",") if x.strip().lstrip("-").isdigit()]
            if isinstance(fl_raw, str):
                labels = [x.strip() for x in fl_raw.split(",")]
            if isinstance(hf_raw, (int, float, str)) and f"{hf_raw}".lstrip("-").isdigit():
                home_floor = int(hf_raw)
        except Exception as e:
            logger.error(f"[ATTRIBUTES] fetch/parse failed: {e}")

    if not boundaries:
        boundaries = [0, 3000, 6000, 9000, 12000, 15000, 18000]
    if not labels:
        labels = [str(i) for i in range(max(0, len(boundaries) - 1))]
    labels = labels[: max(0, len(boundaries) - 1)]

    _floor_meta_cache[key] = {"boundaries": boundaries, "labels": labels, "home_floor": home_floor, "ts": now}
    return boundaries, labels, home_floor

# ---- Core math ---------------------------------------------------------------
def _compute_height(parsed: Dict[str, Any], boundaries: list) -> float:
    h = get_float(parsed, "h")
    if h is not None:
        return float(h)
    laser = get_float(parsed, "laser_val")
    if laser is not None and boundaries:
        max_b = float(boundaries[-1])
        return max(0.0, max_b - float(laser))
    hr = get_float(parsed, "height_raw")
    if hr is not None:
        return float(hr)
    return 0.0

def _floor_index(h: float, boundaries: list) -> int:
    if len(boundaries) < 2:
        return 0
    for i in range(len(boundaries) - 1):
        if boundaries[i] <= h < boundaries[i + 1]:
            return i
    return len(boundaries) - 2

def _derive_motion(device: str, h: float) -> Tuple[str, str, float]:
    st = _movement_state.setdefault(device, {})
    prev_h = st.get("prev_h")
    if prev_h is None:
        st["prev_h"] = h
        st["last_ts"] = int(time.time() * 1000)
        return "S", "I", 0.0
    vel = h - float(prev_h)
    if   vel >  MOVEMENT_DEADBAND_MM: dirc, status = "U", "M"
    elif vel < -MOVEMENT_DEADBAND_MM: dirc, status = "D", "M"
    else:                             dirc, status = "S", "I"
    st["prev_h"] = h
    st["last_ts"] = int(time.time() * 1000)
    return dirc, status, vel

# ---- Alarms ------------------------------------------------------------------
def _create_alarm_on_tb(device: str, alarm_type: str, ts_ms: int, severity: str, details: dict, account_id: str):
    dev_id = _get_device_id(device, account_id)
    if not dev_id:
        logger.warning(f"[ALARM] Device ID not found for {device}")
        return
    host = ACCOUNTS[account_id]
    payload = {
        "originator": {"entityType": "DEVICE", "id": dev_id},
        "type": alarm_type,
        "severity": severity,
        "status": "ACTIVE_UNACK",
        "details": details,
    }
    r = requests.post(f"{host}/api/alarm", headers=_admin_headers(account_id), json=payload, timeout=10)
    if 200 <= r.status_code < 300:
        logger.info(f"[ALARM] Created: {alarm_type} ({device})")
    else:
        logger.error(f"[ALARM] Failed {r.status_code}: {r.text}")

def _bucket_check_and_trigger(device: str, metric: str, value: float, h: float, ts_ms: int, account_id: str):
    dev_buckets = _bucket_counts.setdefault(device, {})
    buckets = dev_buckets.setdefault(metric, [])
    matched = False
    for b in list(buckets):
        if abs(b["center"] - h) <= BUCKET_HALF_MM:
            b["count"] += 1
            matched = True
            if b["count"] >= 3:
                _create_alarm_on_tb(
                    device,
                    f"{metric} Alarm",
                    ts_ms,
                    "MINOR",
                    {"value": value, "threshold": THRESHOLDS[metric],
                     "height_zone": f"{b['center']-BUCKET_HALF_MM:.1f}..{b['center']+BUCKET_HALF_MM:.1f} mm"},
                    account_id,
                )
                buckets.remove(b)
            break
    if not matched:
        buckets.append({"center": h, "count": 1})

def _process_door_timers(device: str, door_open_bit: Optional[int], ts_ms: int, account_id: str, floor_label: str):
    now = time.time()
    if door_open_bit is None:
        door_open_bit = 1 if _device_door_state.get(device, False) else 0
    else:
        _device_door_state[device] = bool(door_open_bit)

    if door_open_bit == 1:
        if device not in _door_open_since:
            _door_open_since[device] = now
        else:
            duration = now - _door_open_since[device]
            if duration >= DOOR_OPEN_THRESHOLD_SEC:
                _create_alarm_on_tb(
                    device, "Door Open Too Long", ts_ms, "MAJOR",
                    {"duration_sec": int(duration), "floor": floor_label}, account_id
                )
                _door_open_since.pop(device, None)
    else:
        _door_open_since.pop(device, None)

def _floor_mismatch(height: float, fi: int, boundaries: list) -> Tuple[bool, float, float]:
    if height is None or fi is None:
        return False, 0.0, 0.0
    if fi >= len(boundaries):
        return True, 0.0, 0.0
    floor_center = float(boundaries[fi])
    deviation = height - floor_center
    return abs(deviation) > TOLERANCE_MM, deviation, floor_center

# ---- Endpoint ----------------------------------------------------------------
@router.post("/check_alarm/")
def check_alarm(
    payload: AlarmIn,
    x_account_id: Optional[str] = Header(None, alias="X-Account-Id"),
    authorization: Optional[str] = Header(None),
):
    """
    Rule Chain posts {deviceName, pack_raw (or pack_out/raw/pack/payload), ts?}.
    Evaluates alarms and creates TB alarms via admin JWT.
    """
    account = _resolve_account(x_account_id)

    if not payload.pack_raw:
        logger.error("[/check_alarm] Missing 'pack_raw' (or alias 'pack_out'/'raw'/'pack')")
        raise HTTPException(status_code=400, detail="Missing 'pack_raw'")

    device = payload.deviceName
    parsed = parse_pack_raw(payload.pack_raw)
    ts_ms = payload.ts if payload.ts is not None else (ts_millis(parsed) or int(time.time() * 1000))

    # Floor meta + core deriveds
    boundaries, labels, _home = _get_floor_meta(device, account)
    h = _compute_height(parsed, boundaries)
    fi = _floor_index(h, boundaries)
    fl = labels[fi] if 0 <= fi < len(labels) else str(fi)
    dirc, status, _vel = _derive_motion(device, h)
    door_bit = door_to_bit(parsed.get("door_val"))

    alarm_events: List[Dict[str, Any]] = []
    
    # Environment thresholds
    env_pairs = [
        ("temperature", get_float(parsed, "temperature", get_float(parsed, "mpu_temp_val"))),
        ("humidity", get_float(parsed, "humidity", get_float(parsed, "humidity_val"))),
    ]
    for name, val in env_pairs:
        if val is not None and name in THRESHOLDS and float(val) > THRESHOLDS[name]:
            alarm_events.append({"code": f"{name.upper()}_HIGH", "severity": "WARNING", "value": float(val)})
            _create_alarm_on_tb(
                device, f"{name.capitalize()} Alarm", ts_ms, "WARNING",
                {"value": float(val), "threshold": THRESHOLDS[name], "floor": fl}, account
            )

    # Vibe/Jerk thresholds with 3-hit bucket logic
    metric_map = {
        "x_vibe": get_float(parsed, "x_vibe", get_float(parsed, "accel_x_val")),
        "y_vibe": get_float(parsed, "y_vibe", get_float(parsed, "accel_y_val")),
        "z_vibe": get_float(parsed, "z_vibe", get_float(parsed, "accel_z_val")),
        "x_jerk": get_float(parsed, "x_jerk", get_float(parsed, "gyro_x_val")),
        "y_jerk": get_float(parsed, "y_jerk", get_float(parsed, "gyro_y_val")),
        "z_jerk": get_float(parsed, "z_jerk", get_float(parsed, "gyro_z_val")),
    }
    for metric, val in metric_map.items():
        if val is not None and metric in THRESHOLDS and float(val) > THRESHOLDS[metric]:
            _bucket_check_and_trigger(device, metric, float(val), float(h), ts_ms, account)

    # Door open while moving
    if status == "M" and door_bit == 1:
        alarm_events.append({"code": "DOOR_OPEN_WHILE_MOVING", "severity": "CRITICAL", "fi": fi})
        _create_alarm_on_tb(
            device, "Door Open While Moving", ts_ms, "CRITICAL",
            {"fi": fi, "floor": fl, "direction": dirc, "h": round(h)}, account
        )

    # Door-open too long
    _process_door_timers(device, door_bit, ts_ms, account, fl)

    # Floor mismatch while door open
    if door_bit == 1:
        mismatch, deviation, center = _floor_mismatch(float(h), int(fi), boundaries)
        if mismatch:
            pos = "above" if deviation > 0 else "below"
            alarm_events.append({"code": "FLOOR_MISMATCH", "severity": "CRITICAL", "pos": pos, "dev_mm": abs(deviation)})
            _create_alarm_on_tb(
                device, "Floor Mismatch Alarm", ts_ms, "CRITICAL",
                {"reported_index": fi, "height": round(h), "deviation_mm": round(abs(deviation), 1),
                 "position": pos, "center": center, "floor": fl},
                account,
            )

    logger.info(f"[ALARM] {device} events={len(alarm_events)} fi={fi} fl={fl} dir={dirc} st={status} h={round(h)}")
    return {"status": "processed", "alarm_events": alarm_events}