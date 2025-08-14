from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Union, Tuple
from datetime import datetime
import requests
import os
import logging
import time
import json
from thingsboard_auth import get_admin_jwt 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

try:
    ACCOUNTS = json.loads(os.getenv("TB_ACCOUNTS", '{}'))
    if not isinstance(ACCOUNTS, dict):
        raise ValueError("TB_ACCOUNTS must be a JSON object")
except json.JSONDecodeError:
    raise RuntimeError("Invalid JSON format for TB_ACCOUNTS environment variable")

logger.info(f"[INIT] Loaded ThingsBoard accounts: {list(ACCOUNTS.keys())}")

THRESHOLDS = {
    "humidity": 50.0,
    "temperature": 50.0,
    "x_jerk": 5.0,
    "y_jerk": 5.0,
    "z_jerk": 15.0,
    "x_vibe": 5.0,
    "y_vibe": 5.0,
    "z_vibe": 15.0
}

TOLERANCE_MM = 10.0
DOOR_OPEN_THRESHOLD_SEC = 15

class TelemetryPayload(BaseModel):
    deviceName: str = Field(...)
    floor: str = Field(...)
    timestamp: str = Field(...)
    height: Optional[Union[float, str]] = Field(default=None)
    current_floor_index: Optional[Union[int, str]] = Field(default=None)
    x_vibe: Optional[Union[float, str]] = Field(default=None)
    y_vibe: Optional[Union[float, str]] = Field(default=None)
    z_vibe: Optional[Union[float, str]] = Field(default=None)
    x_jerk: Optional[Union[float, str]] = Field(default=None)
    y_jerk: Optional[Union[float, str]] = Field(default=None)
    z_jerk: Optional[Union[float, str]] = Field(default=None)
    temperature: Optional[Union[float, str]] = Field(default=None)
    humidity: Optional[Union[float, str]] = Field(default=None)
    door_open: Optional[Union[bool, str]] = Field(default=None)

device_cache = {}
bucket_counts = {}
device_door_state = {}
door_open_since = {}

def parse_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def get_device_id(device_name: str, account_id: str) -> Optional[str]:
    cache_key = f"{account_id}:{device_name}"
    if cache_key in device_cache:
        return device_cache[cache_key]

    token = get_admin_jwt(account_id, ACCOUNTS[account_id])
    host = ACCOUNTS[account_id]
    url = f"{host}/api/tenant/devices?deviceName={device_name}"
    res = requests.get(url, headers={"X-Authorization": f"Bearer {token}"})
    logger.info(f"[DEVICE_LOOKUP] Fetching ID for {device_name} ({account_id}) | Status: {res.status_code}")

    if res.status_code == 200:
        try:
            device_id = res.json()["id"]["id"]
            device_cache[cache_key] = device_id
            return device_id
        except Exception as e:
            logger.error(f"[DEVICE_LOOKUP] Failed to parse device ID: {e}")
    else:
        logger.error(f"[DEVICE_LOOKUP] Failed: {res.status_code} | {res.text}")
    return None

def get_floor_boundaries(device_id: str, account_id: str) -> Optional[str]:
    token = get_admin_jwt(account_id, ACCOUNTS[account_id])
    host = ACCOUNTS[account_id]
    url = f"{host}/api/plugins/telemetry/DEVICE/{device_id}/values/attributes/SERVER_SCOPE"
    res = requests.get(url, headers={"X-Authorization": f"Bearer {token}"})
    logger.info(f"[ATTRIBUTES] Fetching floor boundaries ({account_id}) | Status: {res.status_code}")

    if res.status_code == 200:
        try:
            for attr in res.json():
                if attr["key"] == "floor_boundaries":
                    return attr["value"]
        except Exception as e:
            logger.error(f"[ATTRIBUTES] Failed to parse attributes: {e}")
    return None

def create_alarm_on_tb(device_name: str, alarm_type: str, ts: int, severity: str, details: dict, account_id: str):
    device_id = get_device_id(device_name, account_id)
    if not device_id:
        logger.warning(f"[ALARM] Could not fetch device ID for {device_name}")
        return

    token = get_admin_jwt(account_id, ACCOUNTS[account_id])
    host = ACCOUNTS[account_id]
    alarm_payload = {
        "originator": {
            "entityType": "DEVICE",
            "id": device_id
        },
        "type": alarm_type,
        "severity": severity,
        "status": "ACTIVE_UNACK",
        "details": details
    }
    response = requests.post(
        f"{host}/api/alarm",
        headers={"X-Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=alarm_payload
    )
    if 200 <= response.status_code < 300:
        logger.info(f"[ALARM] Created: {alarm_payload}")
    else:
        logger.error(f"[ALARM] Failed: {response.status_code} - {response.text}")

def check_bucket_and_trigger(device: str, key: str, value: float, height: float, ts: int, floor: str, account_id: str):
    if device not in bucket_counts:
        bucket_counts[device] = {}
    if key not in bucket_counts[device]:
        bucket_counts[device][key] = []

    buckets = bucket_counts[device][key]
    matched = False

    for b in buckets:
        if abs(b["center"] - height) <= 50:
            b["count"] += 1
            matched = True
            if b["count"] >= 3:
                create_alarm_on_tb(device, f"{key} Alarm", ts, "MINOR", {
                    "value": value,
                    "threshold": THRESHOLDS[key],
                    "floor": floor,
                    "height_zone": f"{b['center']-50:.1f} to {b['center']+50:.1f}"
                }, account_id)
                buckets.remove(b)
            break

    if not matched:
        buckets.append({"center": height, "count": 1})

def process_door_alarm(device_name: str, door_open: Optional[bool], floor: str, ts: int, account_id: str):
    now = time.time()
    if door_open is None:
        door_open = device_door_state.get(device_name, False)
    else:
        device_door_state[device_name] = door_open

    if door_open:
        if device_name not in door_open_since:
            door_open_since[device_name] = now
        else:
            duration = now - door_open_since[device_name]
            if duration >= DOOR_OPEN_THRESHOLD_SEC:
                create_alarm_on_tb(device_name, "Door Open Too Long", ts, "MAJOR", {
                    "duration_sec": int(duration),
                    "floor": floor
                }, account_id)
                door_open_since.pop(device_name, None)
    else:
        door_open_since.pop(device_name, None)

def floor_mismatch_detected(height: float, current_floor_index: int, floor_boundaries_str: str) -> Tuple[bool, float, float]:
    try:
        if height is None or current_floor_index is None:
            return False, 0, 0

        floor_boundaries = [float(x.strip()) for x in floor_boundaries_str.split(",") if x.strip()]
        if current_floor_index >= len(floor_boundaries):
            return True, 0, 0

        floor_center = floor_boundaries[current_floor_index]
        deviation = height - floor_center
        return abs(deviation) > TOLERANCE_MM, deviation, floor_center

    except Exception as e:
        logger.error(f"[ERROR] Floor mismatch logic failed: {e}")
        return False, 0, 0

@router.post("/check_alarm/")
async def check_alarm(
    payload: TelemetryPayload,
    x_account_id: str = Header(...),
    authorization: Optional[str] = Header(None)
):
    logger.info("--- /check_alarm/ invoked ---")
    logger.info(f"Payload received: {payload}")

    if x_account_id not in ACCOUNTS:
        raise HTTPException(status_code=400, detail="Invalid account ID")

    ts = int(datetime.utcnow().timestamp() * 1000)
    triggered = []

    try:
        height = parse_float(payload.height)
        current_floor_index = int(payload.current_floor_index) if payload.current_floor_index is not None else None

        for k in ["humidity", "temperature"]:
            val = parse_float(getattr(payload, k))
            if val is not None and val > THRESHOLDS[k]:
                triggered.append({
                    "type": f"{k.capitalize()} Alarm",
                    "value": val,
                    "threshold": THRESHOLDS[k],
                    "severity": "WARNING"
                })
                create_alarm_on_tb(payload.deviceName, f"{k.capitalize()} Alarm", ts, "WARNING", {
                    "value": val,
                    "threshold": THRESHOLDS[k],
                    "floor": payload.floor
                }, x_account_id)

        for key in ["x_jerk", "y_jerk", "z_jerk", "x_vibe", "y_vibe", "z_vibe"]:
            val = parse_float(getattr(payload, key))
            if val is not None and val > THRESHOLDS[key]:
                check_bucket_and_trigger(payload.deviceName, key, val, height, ts, payload.floor, x_account_id)

        is_door_open = payload.door_open or device_door_state.get(payload.deviceName, False)
        if current_floor_index is not None and is_door_open:
            device_id = get_device_id(payload.deviceName, x_account_id)
            if device_id:
                floor_boundaries = get_floor_boundaries(device_id, x_account_id)
                if floor_boundaries:
                    mismatch, deviation, floor_center = floor_mismatch_detected(height, current_floor_index, floor_boundaries)
                    if mismatch:
                        position = "above" if deviation > 0 else "below"
                        triggered.append({
                            "type": "Floor Mismatch Alarm",
                            "value": height,
                            "severity": "CRITICAL",
                            "position": position
                        })
                        create_alarm_on_tb(payload.deviceName, "Floor Mismatch Alarm", ts, "CRITICAL", {
                            "reported_index": current_floor_index,
                            "height": height,
                            "boundaries": floor_boundaries,
                            "deviation_mm": abs(deviation),
                            "position": position
                        }, x_account_id)

        process_door_alarm(payload.deviceName, payload.door_open, payload.floor, ts, x_account_id)

        logger.info(f"Triggered alarms: {triggered}")
        return {"status": "processed", "alarms_triggered": triggered}

    except Exception as e:
        logger.error(f"[ERROR] Exception during alarm processing: {e}")
        raise HTTPException(status_code=500, detail="Alarm processing failed")
