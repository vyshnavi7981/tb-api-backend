

import os
from dotenv import load_dotenv

load_dotenv()

TB_ACCOUNTS = {
    "account1": os.getenv("ACCOUNT1_BASE_URL", "https://thingsboard.cloud"),
    "account2": os.getenv("ACCOUNT2_BASE_URL", "https://thingsboard.cloud"),
    "account3": os.getenv("ACCOUNT3_BASE_URL", "https://thingsboard.cloud")
}

ACCOUNT1_ADMIN_USER = os.getenv("ACCOUNT1_ADMIN_USER")
ACCOUNT1_ADMIN_PASS = os.getenv("ACCOUNT1_ADMIN_PASS")

ACCOUNT2_ADMIN_USER = os.getenv("ACCOUNT2_ADMIN_USER")
ACCOUNT2_ADMIN_PASS = os.getenv("ACCOUNT2_ADMIN_PASS")

ACCOUNT3_ADMIN_USER = os.getenv("ACCOUNT3_ADMIN_USER")
ACCOUNT3_ADMIN_PASS = os.getenv("ACCOUNT3_ADMIN_PASS")
