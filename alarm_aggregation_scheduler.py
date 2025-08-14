import time
import os
import requests
import logging
import threading
from thingsboard_auth import get_admin_jwt
from config import TB_ACCOUNTS
from datetime import datetime, timedelta

SCAN_INTERVAL = int(os.getenv("TB_SCHEDULER_INTERVAL", "30"))  

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("alarm_scheduler")

stop_event = threading.Event()

def scheduler():
    logger.info("[Scheduler] Starting alarm aggregation loop...")
    while not stop_event.is_set():
        try:
            for account_id, base_url in TB_ACCOUNTS.items():
                jwt_token = get_admin_jwt(account_id, base_url)
                if not jwt_token:
                    logger.error(f"[Scheduler] Failed to get admin JWT for {account_id}, skipping...")
                    continue

                headers = {"X-Authorization": f"Bearer {jwt_token}"}
                all_assets = get_all_assets(base_url, headers)

                for asset in all_assets:
                    asset_id = asset['id']['id']
                    count = aggregate_alarm_count(base_url, asset_id, headers)
                    update_asset_alarm_count(base_url, asset_id, count, headers)

        except Exception as e:
            logger.error(f"[Scheduler] Error during aggregation: {e}")

        stop_event.wait(SCAN_INTERVAL)

    logger.info("[Scheduler] Stopped gracefully.")

def get_all_assets(base_url, headers):
    logger.info("[Assets] Fetching all assets...")
    url = f"{base_url}/api/tenant/assets?pageSize=500&page=0"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get("data", [])

def aggregate_alarm_count(base_url, entity_id, headers):
    total = 0
    children = get_related_entities(base_url, entity_id, headers)

    for child in children:
        child_id = child['to']['id']
        entity_type = child['to']['entityType']

        if entity_type == 'DEVICE':
            count = get_device_active_alarm_count(base_url, child_id, headers)
            total += count
        elif entity_type == 'ASSET':
            total += aggregate_alarm_count(base_url, child_id, headers)

    return total

def get_related_entities(base_url, entity_id, headers):
    url = f"{base_url}/api/relations?fromId={entity_id}&fromType=ASSET"
    try:
        resp = requests.get(url, headers=headers, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.warning(f"[Relations] Failed for {entity_id}: {e}")
        return []

def get_device_active_alarm_count(base_url, device_id, headers):
    url = f"{base_url}/api/alarm/DEVICE/{device_id}"
    params = {
        "pageSize": 100,
        "page": 0,
        "searchStatus": "ACTIVE"
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        active_alarms = [
            alarm for alarm in data.get("data", [])
            if alarm.get("status") in ["ACTIVE_UNACK", "ACTIVE_ACK"]
        ]

        logger.info(f"[Alarms] Device {device_id} has {len(active_alarms)} active alarms")
        return len(active_alarms)

    except requests.RequestException as e:
        logger.warning(f"[Alarms] Failed to get alarms for device {device_id}: {e}")
        return 0

def update_asset_alarm_count(base_url, asset_id, count, headers):
    url = f"{base_url}/api/plugins/telemetry/ASSET/{asset_id}/SERVER_SCOPE"
    body = {
        "active_child_alarms": count,
        "has_critical_alarm": count > 0
    }
    try:
        resp = requests.post(url, headers={**headers, "Content-Type": "application/json"}, json=body)
        resp.raise_for_status()
        logger.info(f"[Update] Asset {asset_id} updated with count={count}")
    except requests.RequestException as e:
        logger.warning(f"[Update] Failed to update asset {asset_id}: {e}")

def stop_scheduler():
    logger.info("[Scheduler] Stop signal received.")
    stop_event.set()
