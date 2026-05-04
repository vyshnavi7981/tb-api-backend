import logging
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
import time
import os
import json
import requests

from vibrationcode import process_vibration

router = APIRouter()
logger = logging.getLogger("calculated_telemetry")

# Load accounts
try:
    ACCOUNTS = json.loads(os.getenv("TB_ACCOUNTS", '{}'))
    if not isinstance(ACCOUNTS, dict):
        raise ValueError("TB_ACCOUNTS must be a JSON object")
except json.JSONDecodeError:
    raise RuntimeError("Invalid JSON format for TB_ACCOUNTS environment variable")

logger.info(f"[INIT] Loaded ThingsBoard accounts: {list(ACCOUNTS.keys())}")

# In-memory state
device_state = {}
floor_door_counts = {}
floor_door_durations = {}

# Payload Model
class TelemetryPayload(BaseModel):
    deviceName: str = Field(...)
    device_token: str = Field(...)
    current_floor_index: int = Field(...)
    lift_status: str = Field(...)
    door_open: Optional[bool] = False
    ts: Optional[int] = None

    # Vibration inputs
    x: Optional[float] = 0
    y: Optional[float] = 0
    z: Optional[float] = 0


@router.post("/calculated-telemetry/")
async def calculate_telemetry(
    payload: TelemetryPayload,
    x_account_id: str = Header(...)
):
    logger.info("--- /calculated-telemetry/ invoked ---")
    logger.info(f"Payload: {payload}")

    if x_account_id not in ACCOUNTS:
        raise HTTPException(status_code=400, detail="Invalid account ID")

    ts = payload.ts or int(time.time() * 1000)
    current_time = ts // 1000
    device_key = f"{x_account_id}:{payload.device_token}"
    floor = int(payload.current_floor_index)

    # Initialize state
    if device_key not in device_state:
        device_state[device_key] = {
            "last_idle_home_ts": None,
            "total_idle_home": 0,
            "last_idle_outside_ts": None,
            "total_idle_outside": 0,
            "last_floor": floor,
            "prev_acc_mag": None
        }

    if device_key not in floor_door_counts:
        floor_door_counts[device_key] = {}

    if device_key not in floor_door_durations:
        floor_door_durations[device_key] = {}

    state = device_state[device_key]
    home_floor = 1

    # Idle logic
    is_idle = payload.lift_status.lower() == "idle"

    if is_idle:
        if floor == home_floor:
            if state["last_idle_home_ts"] is None:
                state["last_idle_home_ts"] = current_time
            else:
                elapsed = current_time - state["last_idle_home_ts"]
                state["total_idle_home"] += elapsed
                state["last_idle_home_ts"] = current_time
            state["last_idle_outside_ts"] = None
        else:
            if state["last_idle_outside_ts"] is None:
                state["last_idle_outside_ts"] = current_time
            else:
                elapsed = current_time - state["last_idle_outside_ts"]
                state["total_idle_outside"] += elapsed
                state["last_idle_outside_ts"] = current_time
            state["last_idle_home_ts"] = None
    else:
        state["last_idle_home_ts"] = None
        state["last_idle_outside_ts"] = None

    # Floor init
    if floor not in floor_door_counts[device_key]:
        floor_door_counts[device_key][floor] = 0

    if floor not in floor_door_durations[device_key]:
        floor_door_durations[device_key][floor] = 0

    # Door logic
    if payload.door_open:
        floor_door_counts[device_key][floor] += 1
        last_ts_key = f"last_open_ts_{floor}"
        if last_ts_key not in state:
            state[last_ts_key] = current_time
    else:
        last_ts_key = f"last_open_ts_{floor}"
        if last_ts_key in state:
            open_duration = current_time - state[last_ts_key]
            floor_door_durations[device_key][floor] += open_duration
            del state[last_ts_key]

    # Base calculated values
    calculated_values = {
        "idle_home_streak": (
            current_time - state["last_idle_home_ts"] if state["last_idle_home_ts"] else 0
        ),
        "total_idle_home_seconds": state["total_idle_home"],
        "idle_outside_home_streak": (
            current_time - state["last_idle_outside_ts"] if state["last_idle_outside_ts"] else 0
        ),
        "total_idle_outside_home_seconds": state["total_idle_outside"],
        "door_open_count_per_floor": floor_door_counts[device_key],
        "door_open_duration_per_floor": floor_door_durations[device_key],
    }

    # =========================
    # VIBRATION LOGIC
    # =========================
    vib_msg = {
        "x": payload.x,
        "y": payload.y,
        "z": payload.z
    }

    metadata = {
        "prev_acc_mag": state.get("prev_acc_mag"),
        "vibration_threshold": 0.08
    }

    vib_result = process_vibration(vib_msg, metadata)

    # Store for next request
    state["prev_acc_mag"] = vib_result["msg"]["acc_mag"]

    # Add vibration results
    calculated_values.update({
        "acc_mag": vib_result["msg"]["acc_mag"],
        "vibration_score": vib_result["msg"]["vibration_score"],
        "is_vibrating": vib_result["msg"]["is_vibrating"],
        "vibration_level": vib_result["msg"]["vibration_level"]
    })

    # SEND TO THINGSBOARD
    tb_url = f"https://thingsboard.cloud/api/v1/{payload.device_token}/telemetry"

    try:
        res = requests.post(tb_url, json=calculated_values, timeout=10)
        logger.info(f"TB response: {res.status_code} {res.text}")
    except Exception as e:
        logger.error(f"TB send failed: {e}")

    return {
        "status": "success",
        "calculated": calculated_values
    }


@router.get("/calculated-telemetry/")
def calculated_telemetry_info():
    return {
        "status": "ready",
        "description": "Send POST requests with telemetry payload and X-Account-Id header.",
        "example_payload": {
            "deviceName": "Device 1",
            "device_token": "abc123",
            "current_floor_index": 1,
            "lift_status": "idle",
            "door_open": False,
            "x": 0.02,
            "y": 0.01,
            "z": 0.98,
            "ts": 1714521600000
        },
        "required_headers": ["X-Account-Id"]
    }
