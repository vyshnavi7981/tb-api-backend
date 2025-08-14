import logging
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
import time
import os
import json
from thingsboard_auth import get_admin_jwt  

router = APIRouter()
logger = logging.getLogger("calculated_telemetry")


try:
    ACCOUNTS = json.loads(os.getenv("TB_ACCOUNTS", '{}'))
    if not isinstance(ACCOUNTS, dict):
        raise ValueError("TB_ACCOUNTS must be a JSON object")
except json.JSONDecodeError:
    raise RuntimeError("Invalid JSON format for TB_ACCOUNTS environment variable")

logger.info(f"[INIT] Loaded ThingsBoard accounts: {list(ACCOUNTS.keys())}")


device_state = {}  
floor_door_counts = {}  
floor_door_durations = {}  

from pydantic import BaseModel, Field
from typing import Optional

class TelemetryPayload(BaseModel):
    deviceName: str = Field(...)
    device_token: str = Field(...)
    current_floor_index: int = Field(...)
    lift_status: str = Field(...)
    door_open: Optional[bool] = Field(default=False)
    ts: Optional[int] = Field(default=None)


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
            "last_status": None,
            "last_floor": floor
        }

    if device_key not in floor_door_counts:
        floor_door_counts[device_key] = {}

    if device_key not in floor_door_durations:
        floor_door_durations[device_key] = {}

    state = device_state[device_key]
    home_floor = 1  # TODO: 

   
    is_idle = (payload.lift_status.lower() == "idle") or payload.door_open

 
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

    
    if floor not in floor_door_counts[device_key]:
        floor_door_counts[device_key][floor] = 0
    if floor not in floor_door_durations[device_key]:
        floor_door_durations[device_key][floor] = 0

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

    return {
        "status": "success",
        "calculated": calculated_values
    }
