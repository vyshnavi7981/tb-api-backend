# report_logic.py
import os
import re
import time
import json
import uuid
import math
import logging
from datetime import datetime, date, timedelta
from typing import List, Optional, Any, Dict, Tuple

import pandas as pd
import requests
from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, field_validator

# Shared parser for packed strings
from pack_format import parse_pack_raw

logger = logging.getLogger("report_logic")
router = APIRouter()

# Where to store generated files on Render (ephemeral but fine for downloads)
REPORT_DIR = os.getenv("REPORT_DIR", "/tmp")
os.makedirs(REPORT_DIR, exist_ok=True)

# --- ThingsBoard account routing (multi-tenant) --------------------------------
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

TB_ACCOUNTS = _load_tb_accounts()
logger.info("[INIT] Loaded ThingsBoard accounts: %s", list(TB_ACCOUNTS.keys()))

def _choose_base_url(x_tb_account: Optional[str]) -> str:
    if x_tb_account:
        if x_tb_account in TB_ACCOUNTS:
            return TB_ACCOUNTS[x_tb_account]
        if x_tb_account.lower() in TB_ACCOUNTS:
            return TB_ACCOUNTS[x_tb_account.lower()]
    return next(iter(TB_ACCOUNTS.values()))

# --- Input types ---------------------------------------------------------------
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

# --- Helpers: date/time parsing ------------------------------------------------
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
    except Exception:
        pass

    raise ValueError(f"unrecognized date format: {val!r}")

# --- Request model -------------------------------------------------------------
class ReportRequest(BaseModel):
    device_name: str = Field(..., alias="deviceName")
    data_types: List[str] = Field(..., alias="dataTypes")
    include_alarms: bool = Field(True, alias="includeAlarms")
    start_date: Any = Field(..., alias="startDate")  # date-like
    end_date: Any = Field(..., alias="endDate")      # date-like

    model_config = {
        "populate_by_name": True,
        "extra": "ignore",
        "str_min_length": 1,
    }

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

# --- TB REST helpers -----------------------------------------------------------
def _tb_headers(jwt: str) -> Dict[str, str]:
    return {"X-Authorization": f"Bearer {jwt}"}

def _tb_get(base: str, path: str, jwt: str, params: Optional[dict] = None):
    url = f"{base.rstrip('/')}{path}"
    r = requests.get(url, headers=_tb_headers(jwt), params=params or {}, timeout=30)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=f"TB GET {path} failed: {r.text}")
    try:
        return r.json()
    except Exception:
        return r.text

def _page_all(fn, *args, page_size=100):
    results = []
    page = 0
    while True:
        data = fn(page=page, pageSize=page_size, *args)
        if isinstance(data, dict):
            chunk = data.get("data") or []
            if isinstance(chunk, list):
                results.extend(chunk)
            has_next = data.get("hasNext", False)
            if not has_next:
                break
            page += 1
        else:
            break
    return results

def _find_device_id(base: str, jwt: str, device_name: str) -> Optional[str]:
    """
    Robust device lookup that works for tenant admins and normal users.
    Tries a few endpoints and falls back to listing visible devices.
    """
    # 1) Tenant admin direct lookup
    try:
        data = _tb_get(base, f"/api/tenant/devices?deviceName={device_name}", jwt)
        if isinstance(data, dict):
            did = (data.get("id") or {}).get("id")
            if isinstance(did, str):
                return did
    except HTTPException:
        pass

    # 2) List visible devices to the user and match by name
    try:
        me = _tb_get(base, "/api/auth/user", jwt)
        authority = str(me.get("authority", "")).upper()
        customer_obj = me.get("customerId") if isinstance(me.get("customerId"), dict) else None
        customer_id = (customer_obj or {}).get("id") if isinstance(customer_obj, dict) else None

        def normalize_devices(items: List[dict]) -> List[Dict[str, str]]:
            out = []
            for d in items:
                did_obj = d.get("id") if isinstance(d.get("id"), dict) else None
                did = (did_obj or {}).get("id") if isinstance(did_obj, dict) else None
                name = d.get("name")
                if isinstance(did, str) and isinstance(name, str):
                    out.append({"id": did, "name": name})
            return out

        if authority == "TENANT_ADMIN":
            def fetch_page(page=0, pageSize=100, **_):
                return _tb_get(base, "/api/tenant/devices", jwt, params={"page": page, "pageSize": pageSize})
            all_devices = _page_all(fetch_page)
        elif customer_id:
            def fetch_page(page=0, pageSize=100, **_):
                return _tb_get(base, f"/api/customer/{customer_id}/devices", jwt, params={"page": page, "pageSize": pageSize})
            all_devices = _page_all(fetch_page)
        else:
            def fetch_page(page=0, pageSize=100, **_):
                return _tb_get(base, "/api/user/devices", jwt, params={"page": page, "pageSize": pageSize})
            all_devices = _page_all(fetch_page)

        for d in normalize_devices(all_devices):
            if d["name"] == device_name:
                return d["id"]
    except Exception as e:
        logger.warning("Device lookup fallback failed: %s", e)

    return None

def _fetch_timeseries_chunks(
    base: str,
    jwt: str,
    device_id: str,
    keys: List[str],
    start_ms: int,
    end_ms: int,
    *,
    chunk_ms: int = 6 * 60 * 60 * 1000,   # 6 hours
    per_call_limit: int = 20000
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch timeseries in chunks to avoid TB limits.
    Returns dict: key -> list of {ts: ms, value: <str|num|bool>}
    """
    out: Dict[str, List[Dict[str, Any]]] = {k: [] for k in keys}
    ks = ",".join(keys)
    cur = start_ms
    while cur <= end_ms:
        window_end = min(end_ms, cur + chunk_ms - 1)
        params = {
            "keys": ks,
            "startTs": cur,
            "endTs": window_end,
            "limit": per_call_limit,
            "agg": "NONE",
            "useStrictDataTypes": "false",
        }
        url = f"/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries"
        try:
            data = _tb_get(base, url, jwt, params)
            if isinstance(data, dict):
                for k in keys:
                    if k in data and isinstance(data[k], list):
                        out[k].extend(data[k])
        except HTTPException as e:
            # If TB has no data for a chunk it may 404—tolerate by skipping
            logger.info("TS fetch chunk %s-%s failed for %s: %s", cur, window_end, ks, e.detail)
        cur = window_end + 1

    # sort each key by ts ascending & de-dup (keep first seen ts)
    for k in keys:
        arr = out[k]
        arr.sort(key=lambda x: int(x.get("ts", 0)))
        dedup = []
        seen_ts = set()
        for p in arr:
            ts = int(p.get("ts", 0))
            if ts in seen_ts:
                continue
            seen_ts.add(ts)
            dedup.append({"ts": ts, "value": p.get("value")})
        out[k] = dedup
    return out

# --- Mapping from packed strings to requested columns -------------------------
def _extract_from_calc_like(pack: str, want: List[str]) -> Dict[str, Any]:
    """
    Parse one calc-like row (pack_calc or pack_out) and return dict of wanted fields.
    Expected short keys: h (height), fi, fl, dir, st.
    """
    parsed = parse_pack_raw(pack)
    out: Dict[str, Any] = {}
    # height
    if "height" in want:
        out["height"] = parsed.get("h")
    # direction (U/D/S or similar)
    if "direction" in want:
        out["direction"] = parsed.get("dir")
    # lift_status (M/I -> moving/idle)
    if "lift_status" in want:
        st = str(parsed.get("st") or "")
        out["lift_status"] = "moving" if st.upper() == "M" else ("idle" if st.upper() == "I" else "")
    # floor index/label
    if "current_floor_index" in want:
        out["current_floor_index"] = parsed.get("fi")
    if "current_floor_label" in want:
        out["current_floor_label"] = parsed.get("fl")
    return out

def _extract_from_pack_raw(pack: str, want: List[str]) -> Dict[str, Any]:
    """
    Parse one pack_raw row and map accelerometer/gyro to vibe/jerk when requested.
    """
    parsed = parse_pack_raw(pack)
    out: Dict[str, Any] = {}

    def pick(mn: str, fallback: str):
        v = parsed.get(mn)
        if v is None:
            v = parsed.get(fallback)
        return v

    if "x_vibe" in want:
        out["x_vibe"] = pick("x_vibe", "accel_x_val")
    if "y_vibe" in want:
        out["y_vibe"] = pick("y_vibe", "accel_y_val")
    if "z_vibe" in want:
        out["z_vibe"] = pick("z_vibe", "accel_z_val")

    if "x_jerk" in want:
        out["x_jerk"] = pick("x_jerk", "gyro_x_val")
    if "y_jerk" in want:
        out["y_jerk"] = pick("y_jerk", "gyro_y_val")
    if "z_jerk" in want:
        out["z_jerk"] = pick("z_jerk", "gyro_z_val")

    return out

# --- File helpers -------------------------------------------------------------
def _safe_filename(base: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", base).strip("._-")
    return s or "report"

def _make_filename(device_name: str, start: date, end: date) -> str:
    base = _safe_filename(f"{device_name}_{start.isoformat()}_{end.isoformat()}")
    return f"{base}_{uuid.uuid4().hex[:8]}.xlsx"

# --- Main endpoint ------------------------------------------------------------
@router.post("/generate_report/")
def generate_report(
    body: ReportRequest,
    authorization: Optional[str] = Header(None, alias="Authorization"),
    x_tb_account: Optional[str] = Header(None, alias="X-TB-Account"),
):
    """
    Generate an Excel report for the requested device and fields.
    - Pulls telemetry from TB using the caller's JWT.
    - Parses 'pack_calc' and/or 'pack_out' for calculated fields; 'pack_raw' for raw fields.
    - Spreads requested keys into separate columns.
    - Returns {filename, download_url}.
    """
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    jwt = authorization.split(" ", 1)[1].strip()
    if not jwt:
        raise HTTPException(status_code=401, detail="Empty JWT")

    if body.end_date < body.start_date:
        raise HTTPException(status_code=400, detail="end_date cannot be before start_date")

    base = _choose_base_url(x_tb_account)
    logger.info(
        "[/generate_report] device=%s types=%s include_alarms=%s start=%s end=%s base=%s",
        body.device_name, body.data_types, body.include_alarms, body.start_date, body.end_date, base,
    )

    # Resolve device id
    device_id = _find_device_id(base, jwt, body.device_name)
    if not device_id:
        raise HTTPException(status_code=404, detail=f"Device '{body.device_name}' not found or not visible to this user")

    # Build time window in ms (full days inclusive, UTC)
    start_dt = datetime.combine(body.start_date, datetime.min.time())
    end_dt = datetime.combine(body.end_date, datetime.max.time().replace(microsecond=0))
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    # Decide which keys to fetch from TB time-series
    need_calc = any(k in body.data_types for k in ("height", "direction", "lift_status", "current_floor_index", "current_floor_label"))
    need_raw  = any(k in body.data_types for k in ("x_vibe", "y_vibe", "z_vibe", "x_jerk", "y_jerk", "z_jerk"))

    keys: List[str] = []
    if need_calc:
        # Your rule chain currently saves 'pack_out'; include 'pack_calc' for backward compatibility.
        keys.extend(["pack_out", "pack_calc"])
    if need_raw:
        keys.append("pack_raw")

    if not keys:
        raise HTTPException(status_code=400, detail="No fetchable keys for the selected data_types")

    # Pull telemetry in chunks
    ts_data = _fetch_timeseries_chunks(base, jwt, device_id, keys, start_ms, end_ms)

    # Build rows by TB timestamp (ms)
    rows_by_ts: Dict[int, Dict[str, Any]] = {}

    def ensure_row(ts_ms: int) -> Dict[str, Any]:
        r = rows_by_ts.get(ts_ms)
        if r is None:
            r = {"ts_ms": ts_ms, "ts_iso": datetime.utcfromtimestamp(ts_ms / 1000.0).isoformat() + "Z"}
            rows_by_ts[ts_ms] = r
        return r

    # Prefer pack_out (new) but also accept pack_calc (legacy) for calculated fields
    if need_calc:
        for key in ("pack_out", "pack_calc"):
            for p in ts_data.get(key, []):
                ts_ms = int(p["ts"])
                val = p.get("value")
                if not isinstance(val, str):
                    continue
                out = _extract_from_calc_like(val, body.data_types)
                if out:
                    row = ensure_row(ts_ms)
                    row.update(out)

    if need_raw:
        for p in ts_data.get("pack_raw", []):
            ts_ms = int(p["ts"])
            val = p.get("value")
            if not isinstance(val, str):
                continue
            out = _extract_from_pack_raw(val, body.data_types)
            if out:
                row = ensure_row(ts_ms)
                row.update(out)

    # Assemble DataFrame
    if not rows_by_ts:
        # Produce an empty but valid file (with header row)
        cols = ["ts_iso", "ts_ms"] + body.data_types
        df = pd.DataFrame(columns=cols)
    else:
        # Order columns: timestamps first, then requested fields in the order provided
        cols = ["ts_iso", "ts_ms"] + body.data_types
        # Materialize rows sorted by ts
        rows = [rows_by_ts[k] for k in sorted(rows_by_ts.keys())]
        df = pd.DataFrame(rows)
        # Make sure all requested cols exist
        for c in cols:
            if c not in df.columns:
                df[c] = pd.NA
        df = df[cols]

    # Meta sheet
    meta = {
        "device_name": body.device_name,
        "data_types": ",".join(body.data_types),
        "include_alarms": body.include_alarms,
        "start_date": body.start_date.isoformat(),
        "end_date": body.end_date.isoformat(),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "account_base_url": base,
        "points_pack_out": len(ts_data.get("pack_out", [])) if "pack_out" in ts_data else 0,
        "points_pack_calc": len(ts_data.get("pack_calc", [])) if "pack_calc" in ts_data else 0,
        "points_pack_raw": len(ts_data.get("pack_raw", [])) if "pack_raw" in ts_data else 0,
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
# report_logic.py
import os
import re
import time
import json
import uuid
import math
import logging
from datetime import datetime, date, timedelta
from typing import List, Optional, Any, Dict, Tuple

import pandas as pd
import requests
from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, field_validator

# Shared parser for packed strings
from pack_format import parse_pack_raw

logger = logging.getLogger("report_logic")
router = APIRouter()

# Where to store generated files on Render (ephemeral but fine for downloads)
REPORT_DIR = os.getenv("REPORT_DIR", "/tmp")
os.makedirs(REPORT_DIR, exist_ok=True)

# --- ThingsBoard account routing (multi-tenant) --------------------------------
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

TB_ACCOUNTS = _load_tb_accounts()
logger.info("[INIT] Loaded ThingsBoard accounts: %s", list(TB_ACCOUNTS.keys()))

def _choose_base_url(x_tb_account: Optional[str]) -> str:
    if x_tb_account:
        if x_tb_account in TB_ACCOUNTS:
            return TB_ACCOUNTS[x_tb_account]
        if x_tb_account.lower() in TB_ACCOUNTS:
            return TB_ACCOUNTS[x_tb_account.lower()]
    return next(iter(TB_ACCOUNTS.values()))

# --- Input types ---------------------------------------------------------------
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

# --- Helpers: date/time parsing ------------------------------------------------
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
    except Exception:
        pass

    raise ValueError(f"unrecognized date format: {val!r}")

# --- Request model -------------------------------------------------------------
class ReportRequest(BaseModel):
    device_name: str = Field(..., alias="deviceName")
    data_types: List[str] = Field(..., alias="dataTypes")
    include_alarms: bool = Field(True, alias="includeAlarms")
    start_date: Any = Field(..., alias="startDate")  # date-like
    end_date: Any = Field(..., alias="endDate")      # date-like

    model_config = {
        "populate_by_name": True,
        "extra": "ignore",
        "str_min_length": 1,
    }

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

# --- TB REST helpers -----------------------------------------------------------
def _tb_headers(jwt: str) -> Dict[str, str]:
    return {"X-Authorization": f"Bearer {jwt}"}

def _tb_get(base: str, path: str, jwt: str, params: Optional[dict] = None):
    url = f"{base.rstrip('/')}{path}"
    r = requests.get(url, headers=_tb_headers(jwt), params=params or {}, timeout=30)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=f"TB GET {path} failed: {r.text}")
    try:
        return r.json()
    except Exception:
        return r.text

def _page_all(fn, *args, page_size=100):
    results = []
    page = 0
    while True:
        data = fn(page=page, pageSize=page_size, *args)
        if isinstance(data, dict):
            chunk = data.get("data") or []
            if isinstance(chunk, list):
                results.extend(chunk)
            has_next = data.get("hasNext", False)
            if not has_next:
                break
            page += 1
        else:
            break
    return results

def _find_device_id(base: str, jwt: str, device_name: str) -> Optional[str]:
    """
    Robust device lookup that works for tenant admins and normal users.
    Tries a few endpoints and falls back to listing visible devices.
    """
    # 1) Tenant admin direct lookup
    try:
        data = _tb_get(base, f"/api/tenant/devices?deviceName={device_name}", jwt)
        if isinstance(data, dict):
            did = (data.get("id") or {}).get("id")
            if isinstance(did, str):
                return did
    except HTTPException:
        pass

    # 2) List visible devices to the user and match by name
    try:
        me = _tb_get(base, "/api/auth/user", jwt)
        authority = str(me.get("authority", "")).upper()
        customer_obj = me.get("customerId") if isinstance(me.get("customerId"), dict) else None
        customer_id = (customer_obj or {}).get("id") if isinstance(customer_obj, dict) else None

        def normalize_devices(items: List[dict]) -> List[Dict[str, str]]:
            out = []
            for d in items:
                did_obj = d.get("id") if isinstance(d.get("id"), dict) else None
                did = (did_obj or {}).get("id") if isinstance(did_obj, dict) else None
                name = d.get("name")
                if isinstance(did, str) and isinstance(name, str):
                    out.append({"id": did, "name": name})
            return out

        if authority == "TENANT_ADMIN":
            def fetch_page(page=0, pageSize=100, **_):
                return _tb_get(base, "/api/tenant/devices", jwt, params={"page": page, "pageSize": pageSize})
            all_devices = _page_all(fetch_page)
        elif customer_id:
            def fetch_page(page=0, pageSize=100, **_):
                return _tb_get(base, f"/api/customer/{customer_id}/devices", jwt, params={"page": page, "pageSize": pageSize})
            all_devices = _page_all(fetch_page)
        else:
            def fetch_page(page=0, pageSize=100, **_):
                return _tb_get(base, "/api/user/devices", jwt, params={"page": page, "pageSize": pageSize})
            all_devices = _page_all(fetch_page)

        for d in normalize_devices(all_devices):
            if d["name"] == device_name:
                return d["id"]
    except Exception as e:
        logger.warning("Device lookup fallback failed: %s", e)

    return None

def _fetch_timeseries_chunks(
    base: str,
    jwt: str,
    device_id: str,
    keys: List[str],
    start_ms: int,
    end_ms: int,
    *,
    chunk_ms: int = 6 * 60 * 60 * 1000,   # 6 hours
    per_call_limit: int = 20000
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch timeseries in chunks to avoid TB limits.
    Returns dict: key -> list of {ts: ms, value: <str|num|bool>}
    """
    out: Dict[str, List[Dict[str, Any]]] = {k: [] for k in keys}
    ks = ",".join(keys)
    cur = start_ms
    while cur <= end_ms:
        window_end = min(end_ms, cur + chunk_ms - 1)
        params = {
            "keys": ks,
            "startTs": cur,
            "endTs": window_end,
            "limit": per_call_limit,
            "agg": "NONE",
            "useStrictDataTypes": "false",
        }
        url = f"/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries"
        try:
            data = _tb_get(base, url, jwt, params)
            if isinstance(data, dict):
                for k in keys:
                    if k in data and isinstance(data[k], list):
                        out[k].extend(data[k])
        except HTTPException as e:
            # If TB has no data for a chunk it may 404—tolerate by skipping
            logger.info("TS fetch chunk %s-%s failed for %s: %s", cur, window_end, ks, e.detail)
        cur = window_end + 1

    # sort each key by ts ascending & de-dup (keep first seen ts)
    for k in keys:
        arr = out[k]
        arr.sort(key=lambda x: int(x.get("ts", 0)))
        dedup = []
        seen_ts = set()
        for p in arr:
            ts = int(p.get("ts", 0))
            if ts in seen_ts:
                continue
            seen_ts.add(ts)
            dedup.append({"ts": ts, "value": p.get("value")})
        out[k] = dedup
    return out

# --- Mapping from packed strings to requested columns -------------------------
def _extract_from_calc_like(pack: str, want: List[str]) -> Dict[str, Any]:
    """
    Parse one calc-like row (pack_calc or pack_out) and return dict of wanted fields.
    Expected short keys: h (height), fi, fl, dir, st.
    """
    parsed = parse_pack_raw(pack)
    out: Dict[str, Any] = {}
    # height
    if "height" in want:
        out["height"] = parsed.get("h")
    # direction (U/D/S or similar)
    if "direction" in want:
        out["direction"] = parsed.get("dir")
    # lift_status (M/I -> moving/idle)
    if "lift_status" in want:
        st = str(parsed.get("st") or "")
        out["lift_status"] = "moving" if st.upper() == "M" else ("idle" if st.upper() == "I" else "")
    # floor index/label
    if "current_floor_index" in want:
        out["current_floor_index"] = parsed.get("fi")
    if "current_floor_label" in want:
        out["current_floor_label"] = parsed.get("fl")
    return out

def _extract_from_pack_raw(pack: str, want: List[str]) -> Dict[str, Any]:
    """
    Parse one pack_raw row and map accelerometer/gyro to vibe/jerk when requested.
    """
    parsed = parse_pack_raw(pack)
    out: Dict[str, Any] = {}

    def pick(mn: str, fallback: str):
        v = parsed.get(mn)
        if v is None:
            v = parsed.get(fallback)
        return v

    if "x_vibe" in want:
        out["x_vibe"] = pick("x_vibe", "accel_x_val")
    if "y_vibe" in want:
        out["y_vibe"] = pick("y_vibe", "accel_y_val")
    if "z_vibe" in want:
        out["z_vibe"] = pick("z_vibe", "accel_z_val")

    if "x_jerk" in want:
        out["x_jerk"] = pick("x_jerk", "gyro_x_val")
    if "y_jerk" in want:
        out["y_jerk"] = pick("y_jerk", "gyro_y_val")
    if "z_jerk" in want:
        out["z_jerk"] = pick("z_jerk", "gyro_z_val")

    return out

# --- File helpers -------------------------------------------------------------
def _safe_filename(base: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", base).strip("._-")
    return s or "report"

def _make_filename(device_name: str, start: date, end: date) -> str:
    base = _safe_filename(f"{device_name}_{start.isoformat()}_{end.isoformat()}")
    return f"{base}_{uuid.uuid4().hex[:8]}.xlsx"

# --- Main endpoint ------------------------------------------------------------
@router.post("/generate_report/")
def generate_report(
    body: ReportRequest,
    authorization: Optional[str] = Header(None, alias="Authorization"),
    x_tb_account: Optional[str] = Header(None, alias="X-TB-Account"),
):
    """
    Generate an Excel report for the requested device and fields.
    - Pulls telemetry from TB using the caller's JWT.
    - Parses 'pack_calc' and/or 'pack_out' for calculated fields; 'pack_raw' for raw fields.
    - Spreads requested keys into separate columns.
    - Returns {filename, download_url}.
    """
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    jwt = authorization.split(" ", 1)[1].strip()
    if not jwt:
        raise HTTPException(status_code=401, detail="Empty JWT")

    if body.end_date < body.start_date:
        raise HTTPException(status_code=400, detail="end_date cannot be before start_date")

    base = _choose_base_url(x_tb_account)
    logger.info(
        "[/generate_report] device=%s types=%s include_alarms=%s start=%s end=%s base=%s",
        body.device_name, body.data_types, body.include_alarms, body.start_date, body.end_date, base,
    )

    # Resolve device id
    device_id = _find_device_id(base, jwt, body.device_name)
    if not device_id:
        raise HTTPException(status_code=404, detail=f"Device '{body.device_name}' not found or not visible to this user")

    # Build time window in ms (full days inclusive, UTC)
    start_dt = datetime.combine(body.start_date, datetime.min.time())
    end_dt = datetime.combine(body.end_date, datetime.max.time().replace(microsecond=0))
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    # Decide which keys to fetch from TB time-series
    need_calc = any(k in body.data_types for k in ("height", "direction", "lift_status", "current_floor_index", "current_floor_label"))
    need_raw  = any(k in body.data_types for k in ("x_vibe", "y_vibe", "z_vibe", "x_jerk", "y_jerk", "z_jerk"))

    keys: List[str] = []
    if need_calc:
        # Your rule chain currently saves 'pack_out'; include 'pack_calc' for backward compatibility.
        keys.extend(["pack_out", "pack_calc"])
    if need_raw:
        keys.append("pack_raw")

    if not keys:
        raise HTTPException(status_code=400, detail="No fetchable keys for the selected data_types")

    # Pull telemetry in chunks
    ts_data = _fetch_timeseries_chunks(base, jwt, device_id, keys, start_ms, end_ms)

    # Build rows by TB timestamp (ms)
    rows_by_ts: Dict[int, Dict[str, Any]] = {}

    def ensure_row(ts_ms: int) -> Dict[str, Any]:
        r = rows_by_ts.get(ts_ms)
        if r is None:
            r = {"ts_ms": ts_ms, "ts_iso": datetime.utcfromtimestamp(ts_ms / 1000.0).isoformat() + "Z"}
            rows_by_ts[ts_ms] = r
        return r

    # Prefer pack_out (new) but also accept pack_calc (legacy) for calculated fields
    if need_calc:
        for key in ("pack_out", "pack_calc"):
            for p in ts_data.get(key, []):
                ts_ms = int(p["ts"])
                val = p.get("value")
                if not isinstance(val, str):
                    continue
                out = _extract_from_calc_like(val, body.data_types)
                if out:
                    row = ensure_row(ts_ms)
                    row.update(out)

    if need_raw:
        for p in ts_data.get("pack_raw", []):
            ts_ms = int(p["ts"])
            val = p.get("value")
            if not isinstance(val, str):
                continue
            out = _extract_from_pack_raw(val, body.data_types)
            if out:
                row = ensure_row(ts_ms)
                row.update(out)

    # Assemble DataFrame
    if not rows_by_ts:
        # Produce an empty but valid file (with header row)
        cols = ["ts_iso", "ts_ms"] + body.data_types
        df = pd.DataFrame(columns=cols)
    else:
        # Order columns: timestamps first, then requested fields in the order provided
        cols = ["ts_iso", "ts_ms"] + body.data_types
        # Materialize rows sorted by ts
        rows = [rows_by_ts[k] for k in sorted(rows_by_ts.keys())]
        df = pd.DataFrame(rows)
        # Make sure all requested cols exist
        for c in cols:
            if c not in df.columns:
                df[c] = pd.NA
        df = df[cols]

    # Meta sheet
    meta = {
        "device_name": body.device_name,
        "data_types": ",".join(body.data_types),
        "include_alarms": body.include_alarms,
        "start_date": body.start_date.isoformat(),
        "end_date": body.end_date.isoformat(),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "account_base_url": base,
        "points_pack_out": len(ts_data.get("pack_out", [])) if "pack_out" in ts_data else 0,
        "points_pack_calc": len(ts_data.get("pack_calc", [])) if "pack_calc" in ts_data else 0,
        "points_pack_raw": len(ts_data.get("pack_raw", [])) if "pack_raw" in ts_data else 0,
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
