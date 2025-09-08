

import os
from dotenv import load_dotenv

load_dotenv()

TB_ACCOUNTS = {
    "account1": os.getenv("ACCOUNT1_BASE_URL", "https://thingsboard.cloud")
    
}

ACCOUNT1_ADMIN_USER = os.getenv("ACCOUNT1_ADMIN_USER")
ACCOUNT1_ADMIN_PASS = os.getenv("ACCOUNT1_ADMIN_PASS")


