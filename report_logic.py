# report_logic.py
import os
import io
import re
import time
import json
import uuid
import logging
from datetime import datetime, date
from typing import List, Optional, Any

import pandas as pd
from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger("report_logic")
router = APIRouter()

# Where to store generated files on Render (ephemeral but fine for downloads)
REPORT_DIR = os.getenv("REPORT_DIR", "/tmp")
os.makedirs(REPORT_DIR, exist_ok=True)

# Telemetry keys we allow in reports (extend as needed)
ALLOWED_TYPES = {
    "height",
    "direction",
    "lift_status",
    "current_floor_index",
    "current_floor_label",
    "x_vibe",
    "y_vibe",
    "z_vibe",
    "x_jerk",
    "y_jerk",
    "z_jerk",
}

def _parse_any_date(val: Any) -> date:
    """
    Accept:
      - 'YYYY-MM-DD'
      - ISO datetime strings
      - epoch millis or seconds (int/str)
    Return Python date (no time component).
    """
    if val is None or val == "":
        raise ValueError("missing date")

    # int-like → epoch
    if isinstance(val, (int, float)) or (isinstance(val, str) and val.isdigit()):
        x = int(val)
        # assume ms if it's too large
        if x > 10_000_000_000:
            dt = datetime.utcfromtimestamp(x / 1000.0)
        else:
            dt = datetime.utcfromtimestamp(x)
        return dt.date()

    if isinstance(val, date) and not isinstance(val, datetime):
        return val

    s = str(val).strip()

    # YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return datetime.strptime(s, "%Y-%m-%d").date()

    # ISO datetime
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.date()
    except Exception as _:
        pass

    raise ValueError(f"unrecognized date format: {val!r}")


class ReportRequest(BaseModel):
    # snake_case (preferred by your widget)
    device_name: str = Field(..., alias="deviceName")
    data_types: List[str] = Field(..., alias="dataTypes")
    include_alarms: bool = Field(True, alias="includeAlarms")
    start_date: Any = Field(..., alias="startDate")
    end_date: Any = Field(..., alias="endDate")

    # Pydantic config: accept both aliases and field names, ignore extras
    model_config = {
        "populate_by_name": True,
        "extra": "ignore",
        "str_min_length": 1,
    }

    # Coerce dates after parsing
    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def _coerce_dates(cls, v):
        return _parse_any_date(v)

    @field_validator("data_types", mode="after")
    @classmethod
    def _filter_types(cls, v: List[str]):
        if not v:
            raise ValueError("data_types cannot be empty")
        filtered = [t for t in v if t in ALLOWED_TYPES]
        if not filtered:
            raise ValueError("No valid data_types provided")
        # Deduplicate while preserving order
        seen = set()
        result = []
        for t in filtered:
            if t not in seen:
                seen.add(t)
                result.append(t)
        return result


def _safe_filename(base: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", base).strip("._-")
    return s or "report"


def _make_filename(device_name: str, start: date, end: date) -> str:
    base = _safe_filename(f"{device_name}_{start.isoformat()}_{end.isoformat()}")
    # Add short uuid to avoid collisions
    return f"{base}_{uuid.uuid4().hex[:8]}.xlsx"


def _fake_rows_for_now(req: ReportRequest) -> pd.DataFrame:
    """
    Placeholder data so downloads work end-to-end immediately.
    Replace with real TB fetch + transform when ready.
    """
    rows = []
    # One row per selected type at start/end to make a minimal but useful sheet
    for t in req.data_types:
        rows.append(
            {
                "timestamp": int(time.mktime(datetime.combine(req.start_date, datetime.min.time()).timetuple())) * 1000,
                "device": req.device_name,
                "key": t,
                "value": None,
                "note": "placeholder row – replace with real data",
            }
        )
        rows.append(
            {
                "timestamp": int(time.mktime(datetime.combine(req.end_date, datetime.min.time()).timetuple())) * 1000,
                "device": req.device_name,
                "key": t,
                "value": None,
                "note": "placeholder row – replace with real data",
            }
        )
    return pd.DataFrame(rows)


@router.post("/generate_report/")
def generate_report(
    body: ReportRequest,
    authorization: Optional[str] = Header(None, alias="Authorization"),
    x_tb_account: Optional[str] = Header(None, alias="X-TB-Account"),
):
    """
    Generate an Excel report for the requested device and fields.
    - Accepts both snake_case and camelCase body keys.
    - Returns {filename, download_url} for the generated file.
    """
    # Basic auth presence check (widget already sends it)
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")

    # Validate dates
    if body.end_date < body.start_date:
        raise HTTPException(status_code=400, detail="end_date cannot be before start_date")

    # You can pass x_tb_account to your internal fetch if needed
    logger.info(
        "[/generate_report] device=%s types=%s include_alarms=%s start=%s end=%s account=%s",
        body.device_name,
        body.data_types,
        body.include_alarms,
        body.start_date,
        body.end_date,
        x_tb_account,
    )

    # === TODO: Replace this block with real data pull from ThingsBoard ===
    df = _fake_rows_for_now(body)
    # ================================================================

    # Add a metadata sheet
    meta = {
        "device_name": body.device_name,
        "data_types": ",".join(body.data_types),
        "include_alarms": body.include_alarms,
        "start_date": body.start_date.isoformat(),
        "end_date": body.end_date.isoformat(),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "account": x_tb_account or "",
    }
    metadata_df = pd.DataFrame([meta])

    # Save to Excel
    filename = _make_filename(body.device_name, body.start_date, body.end_date)
    fpath = os.path.join(REPORT_DIR, filename)
    with pd.ExcelWriter(fpath, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="data")
        metadata_df.to_excel(writer, index=False, sheet_name="meta")

    return {
        "filename": filename,
        "download_url": f"/download/{filename}",
    }


@router.get("/download/{filename}")
def download_report(filename: str):
    """
    Serve a previously generated report from REPORT_DIR.
    """
    safe = _safe_filename(filename)
    if safe != filename:
        
        raise HTTPException(status_code=400, detail="Invalid filename")
    fpath = os.path.join(REPORT_DIR, filename)
    if not os.path.exists(fpath):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(
        fpath,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename,
    )
