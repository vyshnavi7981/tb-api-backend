from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, Optional, Tuple, List

import requests
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field, model_validator

# Common helpers (already in your repo)
from pack_format import (
    parse_pack_raw,
    ts_seconds,
    ts_millis,
    door_to_bit,
    get_float,
)

# Optional live counters (door/idle aggregation without DB reads)
try:
    from live_counters import process_pack_out_sample  # (device_id, device_name, ts_ms, pack_out_str)
except Exception:
    process_pack_out_sample = None  # safe no-op if not present

from thingsboard_auth import get_admin_jwt  # signature: get_admin_jwt(account_id: str, host: str) -> str

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("calculated_telemetry")

router = APIRouter()

# ------------------------------------------------------------------------------
# Multi-account TB endpoints
# ------------------------------------------------------------------------------
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

def _resolve_account(*candidates: Optional[str]) -> str:
    """Pick the first matching account id; fallback to the first configured."""
    for x in candidates:
        if not x:
            continue
        k = str(x).strip()
        if not k:
            continue
        if k in ACCOUNTS:
            return k
        if k.lower() in ACCOUNTS:
            return k.lower()
    return next(iter(ACCOUNTS.keys()))

# ------------------------------------------------------------------------------
# Input model (tolerant)
# ------------------------------------------------------------------------------
class CalcIn(BaseModel):
    deviceName: str = Field(...)
    device_token: Optional[str] = Field(default=None)

    # prefer 'pack_raw', but accept common alternates
    pack_raw: Optional[str] = Field(default=None)
    raw: Optional[str] = Field(default=None)
    pack: Optional[str] = Field(default=None)
    payload: Optional[Dict[str, Any]] = Field(default=None)

    ts: Optional[int] = Field(default=None, description="epoch ms (optional; else parsed ts or now)")

    @model_validator(mode="after")
    def unify_pack(self):
        # Normalize to pack_raw
        if not self.pack_raw:
            if self.raw:
                self.pack_raw = self.raw
            elif self.pack:
                self.pack_raw = self.pack
            elif isinstance(self.payload, dict):
                self.pack_raw = (
                    self.payload.get("pack_raw")
                    or self.payload.get("raw")
                    or self.payload.get("pack")
                )
        return self

# ------------------------------------------------------------------------------
# Caches / state
# ------------------------------------------------------------------------------
_device_id_cache: Dict[str, str] = {}             # f"{account}:{device}" -> deviceId
_floor_meta_cache: Dict[str, Dict[str, Any]] = {} # f"{account}:{device}" -> {boundaries, labels, home_floor, ts}
_movement_state: Dict[str, Dict[str, Any]] = {}   # device -> {"prev_h": float, "last_ts": int}

FLOOR_CACHE_TTL_SEC = 300       # 5 minutes
MOVEMENT_DEADBAND_MM = 20.0     # avoid flapping for tiny height changes

# ------------------------------------------------------------------------------
# TB REST helpers
# ------------------------------------------------------------------------------
def _admin_headers(account_id: str) -> Dict[str, str]:
    host = ACCOUNTS[account_id]
    jwt = get_admin_jwt(account_id, host)
    return {"Content-Type": "application/json", "X-Authorization": f"Bearer {jwt}"}

def _get_device_id(device: str, account_id: str) -> Optional[str]:
    key = f"{account_id}:{device}"
    if key in _device_id_cache:
        return _device_id_cache[key]
    host = ACCOUNTS[account_id]
    url = f"{host}/api/tenant/devices?deviceName={device}"
    r = requests.get(url, headers=_admin_headers(account_id), timeout=10)
    logger.info(f"[DEVICE] lookup {device}@{account_id} -> {r.status_code}")
    if r.ok:
        try:
            dev_id = r.json()["id"]["id"]
            _device_id_cache[key] = dev_id
            return dev_id
        except Exception as e:
            logger.error(f"[DEVICE] parse error: {e}")
    else:
        logger.error(f"[DEVICE] {r.status_code}: {r.text}")
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

def _get_floor_meta(device: str, account_id: str) -> Tuple[List[int], List[str], Optional[int]]:
    """
    Returns (boundaries, labels, home_floor); caches for FLOOR_CACHE_TTL_SEC.
    boundaries: list[int] of floor boundaries/centers in mm
    labels: list[str] (size == len(boundaries)-1)
    """
    key = f"{account_id}:{device}"
    now = time.time()
    cached = _floor_meta_cache.get(key)
    if cached and now - cached.get("ts", 0) < FLOOR_CACHE_TTL_SEC:
        return cached["boundaries"], cached["labels"], cached.get("home_floor")

    dev_id = _get_device_id(device, account_id)
    boundaries: List[int] = []
    labels: List[str] = []
    home_floor: Optional[int] = None
    if dev_id:
        try:
            attrs = _fetch_server_attributes(dev_id, account_id)
            fb_raw = attrs.get("floor_boundaries")  # e.g. "0,3000,6000,..."
            fl_raw = attrs.get("floor_labels")      # e.g. "B3,B2,B1,G,1,2,..."
            hf_raw = attrs.get("home_floor")
            if isinstance(fb_raw, str):
                boundaries = [int(x.strip()) for x in fb_raw.split(",") if x.strip().lstrip("-").isdigit()]
            if isinstance(fl_raw, str):
                labels = [x.strip() for x in fl_raw.split(",")]
            if isinstance(hf_raw, (int, float, str)) and f"{hf_raw}".lstrip("-").isdigit():
                home_floor = int(hf_raw)
        except Exception as e:
            logger.error(f"[ATTR] fetch/parse failed: {e}")

    if not boundaries:
        boundaries = [0, 3000, 6000, 9000, 12000, 15000, 18000]
    if not labels:
        labels = [str(i) for i in range(max(0, len(boundaries) - 1))]
    labels = labels[: max(0, len(boundaries) - 1)]

    _floor_meta_cache[key] = {"boundaries": boundaries, "labels": labels, "home_floor": home_floor, "ts": now}
    return boundaries, labels, home_floor

# ------------------------------------------------------------------------------
# Core math
# ------------------------------------------------------------------------------
def _compute_height(parsed: Dict[str, Any], boundaries: List[int]) -> float:
    """
    Prefer 'h' if present; else 'maxBoundary - laser_val'; else 'height_raw'; else 0.
    """
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

def _floor_index(h: float, boundaries: List[int]) -> int:
    if len(boundaries) < 2:
        return 0
    for i in range(len(boundaries) - 1):
        if boundaries[i] <= h < boundaries[i + 1]:
            return i
    return len(boundaries) - 2

def _derive_motion(device: str, h: float) -> Tuple[str, str, float]:
    """
    Returns (dir='U/D/S', st='M/I', velocity_mm)
    """
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

def _build_pack_calc(ts_sec: int, h: float, fi: int, fl: str, dirc: str, status: str, door_bit: Optional[int]) -> str:
    parts = []
    def add(k, v): parts.append(f"{k}={'' if v is None else v}")
    add("v", 1)
    add("ts", ts_sec)
    add("h", round(h))
    add("fi", fi)
    add("fl", fl)
    add("dir", dirc)    # U/D/S
    add("st", status)   # M/I
    add("door", door_bit)
    return "|".join(parts)

# Which raw fields to preserve (fallbacks included)
_RAW_EXPORT_MAP: List[Tuple[str, Optional[str]]] = [
    ("laser_val", None),
    ("height_raw", None),
    ("x_vibe", "accel_x_val"),
    ("y_vibe", "accel_y_val"),
    ("z_vibe", "accel_z_val"),
    ("x_jerk", "gyro_x_val"),
    ("y_jerk", "gyro_y_val"),
    ("z_jerk", "gyro_z_val"),
    ("temperature", "mpu_temp_val"),
    ("humidity", "humidity_val"),
    ("mic", "mic_val"),
    ("door_val", None),
]

def _build_pack_out(pack_calc: str, parsed_raw: Dict[str, Any], height_mm: float, floor_label: str, door_bit: Optional[int]) -> str:
    """
    Merge calculated string with selected raw fields into a single packed string.
    Adds a few compatibility fields for live_counters:
      - floor_label=<label>
      - height=<mm>
      - door_open=0/1
    """
    parts = [pack_calc]  # start with calc (v, ts, h, fi, fl, dir, st, door)
    # live_counters compatibility fields
    parts.append(f"floor_label={floor_label}")
    parts.append(f"height={int(round(height_mm))}")
    if door_bit is not None:
        parts.append(f"door_open={door_bit}")

    # selected raw fields (with fallbacks)
    for key, fallback in _RAW_EXPORT_MAP:
        val = parsed_raw.get(key)
        if val is None and fallback:
            val = parsed_raw.get(fallback)
        # normalize booleans/None
        if isinstance(val, bool):
            val = "true" if val else "false"
        if val is None:
            continue
        parts.append(f"{key}={val}")

    return "|".join(parts)

# ------------------------------------------------------------------------------
# Endpoint
# ------------------------------------------------------------------------------
@router.post("/calculated-telemetry/")
def calculated_telemetry(
    payload: CalcIn,
    x_account_id: Optional[str] = Header(None, alias="X-Account-Id"),
    x_account_id_alt: Optional[str] = Header(None, alias="X-Account-ID"),
    authorization: Optional[str] = Header(None),  # not used; admin JWT is used for TB reads
):
    """
    Rule Chain posts {deviceName, pack_raw (or raw/pack/payload), ts?}.
    We compute floor/direction/status/door and return:
        {
          "pack_calc": "v=1|ts=...|h=...|fi=...|fl=...|dir=U|st=M|door=1",
          "pack_out":  "pack_calc|<compat + subset of raw k=v pairs>",
          "pack_raw":  "<echo of inbound raw>",
          "ts": <ms>
        }
    Also feeds live_counters (if available) using the enriched pack_out.
    """
    account = _resolve_account(x_account_id, x_account_id_alt)

    if not payload.pack_raw:
        logger.error("[/calculated-telemetry] Missing 'pack_raw' (or 'raw'/'pack')")
        raise HTTPException(status_code=400, detail="Missing 'pack_raw'")

    device = payload.deviceName
    parsed = parse_pack_raw(payload.pack_raw)

    # Timestamp (ms): prefer payload.ts, else parsed ts (seconds), else now
    ts_ms = payload.ts
    if ts_ms is None:
        sec = ts_seconds(parsed)
        ts_ms = int(sec * 1000) if isinstance(sec, int) else (ts_millis(parsed) or int(time.time() * 1000))
    ts_sec = int(ts_ms // 1000)

    # Floor metadata (cached)
    boundaries, labels, _home_floor = _get_floor_meta(device, account)

    # Core deriveds
    h = _compute_height(parsed, boundaries)
    fi = _floor_index(h, boundaries)
    fl = labels[fi] if 0 <= fi < len(labels) else str(fi)
    dirc, status, _vel = _derive_motion(device, h)
    door_bit = door_to_bit(parsed.get("door_val"))

    pack_calc = _build_pack_calc(ts_sec, h, fi, fl, dirc, status, door_bit)
    pack_out = _build_pack_out(pack_calc, parsed, h, fl, door_bit)

    # Feed live counters (optional)
    if process_pack_out_sample:
        try:
            dev_id = _get_device_id(device, account)
            if dev_id:
                process_pack_out_sample(dev_id, device, ts_ms, pack_out)
        except Exception as e:
            logger.exception("[LIVE_COUNTERS] process error for %s: %s", device, e)

    return {
        "pack_calc": pack_calc,
        "pack_out": pack_out,     # <= rule chain stores this
        "pack_raw": payload.pack_raw,
        "ts": ts_ms
    }
