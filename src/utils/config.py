import os
from dotenv import load_dotenv

# Load .env from project root
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_DWH_HOST"),
    "port": os.getenv("POSTGRES_DWH_PORT"),
    "database": os.getenv("POSTGRES_DWH_DB"),
    "user": os.getenv("POSTGRES_DWH_USER"),
    "password": os.getenv("POSTGRES_DWH_PASSWORD"),
}