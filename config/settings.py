from dotenv import load_dotenv
import os

# Load environment variables from a .env file if present. This only needs to
# happen once so we do it at import time before reading any variables.
load_dotenv()

MSSQL_SOURCE_CONN_STR = os.getenv("MSSQL_SOURCE_CONN_STR")
MSSQL_TARGET_CONN_STR = os.getenv("MSSQL_TARGET_CONN_STR")
MYSQL_CONN_DICT = {
    'host': os.getenv("MYSQL_HOST"),
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASSWORD"),
    'database': os.getenv("MYSQL_DATABASE"),
}

