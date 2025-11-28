import os
from dotenv import load_dotenv

load_dotenv()

RUN_ENV = os.getenv("RUN_ENV", "local")

if RUN_ENV == "docker":
    DB_CONFIG = {
        "host": os.getenv("POSTGRES_DWH_HOST"),
        "port": os.getenv("POSTGRES_DWH_PORT"),
        "database": os.getenv("POSTGRES_DWH_DB"),
        "user": os.getenv("POSTGRES_DWH_USER"),
        "password": os.getenv("POSTGRES_DWH_PASSWORD"),
    }
else:
    DB_CONFIG = {
        "host": "localhost",
        "port": os.getenv("POSTGRES_DWH_PORT_EXT"),
        "database": os.getenv("POSTGRES_DWH_DB"),
        "user": os.getenv("POSTGRES_DWH_USER"),
        "password": os.getenv("POSTGRES_DWH_PASSWORD"),
    }