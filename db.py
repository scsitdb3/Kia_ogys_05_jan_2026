import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()

def create_mssql_connection(
    env_server_key="Server",
    env_db_key="Database",
    env_pwd_key="Password",
    username="Utkrishtsa",
    driver="ODBC Driver 17 for SQL Server",
    login_timeout=30,
    query_timeout=120,
    encrypt=False,
    trust_server_cert=True,
    force_tcp=True,
):
    server = os.getenv(env_server_key) or os.getenv(env_server_key.upper())
    database = os.getenv(env_db_key) or os.getenv(env_db_key.upper())
    password = os.getenv(env_pwd_key) or os.getenv(env_pwd_key.upper())

    if not server:
        raise ValueError(f"Missing env: {env_server_key}")
    if not database:
        raise ValueError(f"Missing env: {env_db_key}")
    if not password:
        raise ValueError(f"Missing env: {env_pwd_key}")

    srv = f"tcp:{server}" if force_tcp else server
    enc = "yes" if encrypt else "no"
    tsc = "yes" if trust_server_cert else "no"

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={srv};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Encrypt={enc};"
        f"TrustServerCertificate={tsc};"
        f"Connection Timeout={login_timeout};"
    )

    conn = pyodbc.connect(conn_str)
    conn.timeout = query_timeout
    return conn



