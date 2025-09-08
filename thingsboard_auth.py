import os
import requests
import logging

logger = logging.getLogger("thingsboard_auth")


def login_to_thingsboard(base_url: str, username: str, password: str):
    url = f"{base_url}/api/auth/login"
    payload = {"username": username, "password": password}
    try:
        response = requests.post(url, json=payload, timeout=20)
        response.raise_for_status()
        jwt_token = response.json().get("token")
        if not jwt_token:
            logger.error("[Auth] Login succeeded but no token in response.")
            return None
        return jwt_token
    except requests.exceptions.RequestException as e:
        logger.error(f"[Auth] Failed to retrieve JWT: {e}")
        return None


def get_admin_jwt(account_id: str | None = None, base_url: str | None = None) -> str | None:
    """
    Shared function used by multiple files to get JWT token.

    Defaults:
      - account_id: 'ACCOUNT1' (or whatever you export via env)
      - base_url: env TB_BASE_URL or https://thingsboard.cloud
    """
    account = (account_id or "ACCOUNT1").upper()
    tb_base = base_url or os.getenv("TB_BASE_URL", "https://thingsboard.cloud")

    user_env = f"{account}_ADMIN_USER"
    pass_env = f"{account}_ADMIN_PASS"
    username = os.getenv(user_env)
    password = os.getenv(pass_env)

    if not username or not password:
        logger.warning(f"[Auth] Missing admin credentials in env: {user_env}/{pass_env}")
        return None

    return login_to_thingsboard(tb_base, username, password)
