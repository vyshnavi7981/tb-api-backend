# alarm_aggregation_scheduler.py


import os
import time
import threading
import logging

logger = logging.getLogger("alarm_scheduler")
logging.basicConfig(level=logging.INFO)

# --- Intervals ---
ALARM_INTERVAL_SEC = int(os.getenv("TB_SCHEDULER_INTERVAL", "30"))
FLUSH_INTERVAL_SEC = int(os.getenv("LC_TB_FLUSH_INTERVAL_SEC", "86400"))  # end-of-day in prod
RUN_FLUSH_ON_START = os.getenv("LC_TB_FLUSH_ON_START", "false").lower() in ("1", "true", "yes")

# --- State ---
_stop_event = threading.Event()
_thread = None
_loop_started = False
_state_lock = threading.Lock()

def _alarm_tick():
    logger.info("[AlarmLoop] tick (interval=%ss)", ALARM_INTERVAL_SEC)
    # no-op (your /check_alarm handles real alarm evaluation)

def _current_local_date(ts_ms: int) -> str:
    tz = os.getenv("LC_TZ", "UTC").strip()
    if tz.startswith(("+", "-")) and len(tz) >= 3 and ":" in tz:
        sign = 1 if tz[0] == "+" else -1
        hh, mm = tz[1:].split(":", 1)
        offset_sec = sign * (int(hh) * 3600 + int(mm) * 60)
        sec = (ts_ms // 1000) + offset_sec
        return time.strftime("%Y-%m-%d", time.gmtime(sec))
    return time.strftime("%Y-%m-%d", time.gmtime(ts_ms / 1000.0))

def _flush_tick():
    from live_counters import flush_day_to_tb
    # Flush yesterday if you want to ensure day is complete; here we flush "today so far" (easier for tests)
    now_ms = int(time.time() * 1000)
    date_str = _current_local_date(now_ms)
    logger.info("[LiveCounters] Flushing counters for %s", date_str)
    flushed = flush_day_to_tb(date_str)
    logger.info("[LiveCounters] Flush complete: %d device(s)", flushed)

def _run_loop():
    logger.info("[Scheduler] Starting; alarm every %ss, flush every %ss; flush_on_start=%s",
                ALARM_INTERVAL_SEC, FLUSH_INTERVAL_SEC, RUN_FLUSH_ON_START)

    now = time.time()
    next_alarm = now + ALARM_INTERVAL_SEC
    next_flush = now + FLUSH_INTERVAL_SEC

    if RUN_FLUSH_ON_START:
        time.sleep(3)
        try:
            _flush_tick()
        except Exception as e:
            logger.exception("[LiveCounters] flush on start error: %s", e)
        next_flush = time.time() + FLUSH_INTERVAL_SEC

    while not _stop_event.is_set():
        now = time.time()

        if now >= next_alarm:
            try:
                _alarm_tick()
            except Exception as e:
                logger.exception("[AlarmLoop] error: %s", e)
            finally:
                next_alarm = now + ALARM_INTERVAL_SEC

        if now >= next_flush:
            try:
                _flush_tick()
            except Exception as e:
                logger.exception("[LiveCounters] flush error: %s", e)
            finally:
                next_flush = now + FLUSH_INTERVAL_SEC

        _stop_event.wait(0.5)

    logger.info("[Scheduler] Loop exited")

def scheduler():
    """Blocking variant."""
    global _loop_started
    with _state_lock:
        if _loop_started:
            logger.info("[Scheduler] Already running (blocking)")
            return
        _loop_started = True
        _stop_event.clear()
    try:
        _run_loop()
    finally:
        with _state_lock:
            _loop_started = False
            _stop_event.set()

def start_scheduler():
    """Non-blocking start."""
    global _thread, _loop_started
    with _state_lock:
        if _loop_started and _thread and _thread.is_alive():
            logger.info("[Scheduler] Already running")
            return
        logger.info("[Scheduler] Launching background thread")
        _stop_event.clear()
        _loop_started = True
        _thread = threading.Thread(target=_run_loop, name="tb-scheduler", daemon=True)
        _thread.start()

def stop_scheduler():
    global _thread, _loop_started
    with _state_lock:
        if not _loop_started:
            logger.info("[Scheduler] Not running")
            return
        logger.info("[Scheduler] Stopping...")
        _stop_event.set()
    if _thread:
        _thread.join(timeout=5)
    with _state_lock:
        _thread = None
        _loop_started = False
        logger.info("[Scheduler] Stopped")
