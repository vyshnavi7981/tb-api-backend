# main.py
import os
import json
import threading
import logging
from typing import Dict, List, Optional

import requests
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

# --- App ---
app = FastAPI(title="TB API Backend", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Supports either:
#  - TB_ACCOUNTS='{"account1":"https://thingsboard.cloud","account2":"https://thingsboard.cloud"}'
#  - TB_BASE_URL='https://thingsboard.cloud' (fallback)
def load_tb_accounts() -> Dict[str, str]:
    raw = os.getenv("TB_ACCOUNTS", "").strip()
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict) and data:
                return {str(k): str(v) for k, v in data.items()}
        except Exception as e:
            logger.warning("Failed to parse TB_ACCOUNTS: %s", e)

    base = os.getenv("TB_BASE_URL", "https://thingsboard.cloud").strip()
    return {"default": base}

TB_ACCOUNTS = load_tb_accounts()
logger.info("[INIT] Loaded ThingsBoard accounts: %s", list(TB_ACCOUNTS.keys()))


def try_include_router(module_name: str, attr: str = "router"):
    try:
        mod = __import__(module_name, fromlist=[attr])
        router = getattr(mod, attr, None)
        if router:
            app.include_router(router)
            logger.info("Included router from %s.%s", module_name, attr)
    except Exception as e:
        logger.info("Router %s not included (%s)", module_name, e)

try_include_router("report_logic")
try_include_router("alarm_logic")
try_include_router("calculated_telemetry")


def start_alarm_scheduler():
    try:
        import alarm_aggregation_scheduler as sched
        logger.info("[Scheduler] Starting background scheduler thread...")
        t = threading.Thread(target=sched.scheduler, name="alarm_scheduler", daemon=True)
        t.start()
    except Exception as e:
        logger.error("Failed to start alarm scheduler: %s", e)

@app.on_event("startup")
def on_startup():
    start_alarm_scheduler()

# ===== Helpers =====
def choose_base_url(x_tb_account: Optional[str]) -> str:
    if x_tb_account and x_tb_account in TB_ACCOUNTS:
        return TB_ACCOUNTS[x_tb_account]
    if x_tb_account and x_tb_account.lower() in TB_ACCOUNTS:
        return TB_ACCOUNTS[x_tb_account.lower()]
    return next(iter(TB_ACCOUNTS.values()))

def tb_get(base: str, path: str, jwt: str, params: Optional[dict] = None):
    url = f"{base.rstrip('/')}{path}"
    headers = {"X-Authorization": f"Bearer {jwt}"}
    r = requests.get(url, headers=headers, params=params or {}, timeout=20)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=f"TB GET {path} failed: {r.text}")
    return r.json()

def page_all(fn, *args, page_size=100):
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


@app.get("/")
def root():
    raise HTTPException(status_code=404, detail="Nothing to see here.")

@app.get("/my_devices/")
def get_my_devices(
    authorization: Optional[str] = Header(None, alias="Authorization"),
    x_tb_account: Optional[str] = Header(None, alias="X-TB-Account"),
):
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    jwt = authorization.split(" ", 1)[1].strip()
    if not jwt:
        raise HTTPException(status_code=401, detail="Empty JWT")

    base = choose_base_url(x_tb_account)
    logger.info("[/my_devices] Using base URL: %s", base)

    me = tb_get(base, "/api/auth/user", jwt)
    if not isinstance(me, dict):
        raise HTTPException(status_code=500, detail="Unexpected /api/auth/user response")

    authority = str(me.get("authority", "")).upper()
    customer_obj = me.get("customerId") if isinstance(me.get("customerId"), dict) else None
    customer_id = (customer_obj or {}).get("id") if isinstance(customer_obj, dict) else None

    devices: List[Dict[str, str]] = []

    def normalize_devices(items: List[dict]) -> List[Dict[str, str]]:
        out = []
        for d in items:
            if not isinstance(d, dict):
                continue
            did_obj = d.get("id") if isinstance(d.get("id"), dict) else None
            did = (did_obj or {}).get("id") if isinstance(did_obj, dict) else None
            name = d.get("name")
            if isinstance(did, str) and isinstance(name, str):
                out.append({"id": did, "name": name})
        return out

    if authority == "TENANT_ADMIN":
        def fetch_page(page=0, pageSize=100, **_):
            return tb_get(base, "/api/tenant/devices", jwt, params={"page": page, "pageSize": pageSize})
        all_devices = page_all(fetch_page)
        devices = normalize_devices(all_devices)
    elif customer_id:
        def fetch_page(page=0, pageSize=100, **_):
            return tb_get(base, f"/api/customer/{customer_id}/devices", jwt, params={"page": page, "pageSize": pageSize})
        all_devices = page_all(fetch_page)
        devices = normalize_devices(all_devices)
    else:
        def fetch_page(page=0, pageSize=100, **_):
            return tb_get(base, "/api/user/devices", jwt, params={"page": page, "pageSize": pageSize})
        all_devices = page_all(fetch_page)
        devices = normalize_devices(all_devices)

    return devices

@app.get("/healthz")
def healthz():
    return {"status": "ok"}
