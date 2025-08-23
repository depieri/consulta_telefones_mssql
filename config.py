# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# Informações de conexão com o banco de dados MSSQL
DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_NAME = os.environ.get("DB_NAME")
DB_PORT = os.environ.get("DB_PORT")       # porta padrão do MSSQL
ENCRYPT= os.environ.get("ENCRYPT")
TRUST_SERVER_CERTIFICATE = os.environ.get("TRUST_SERVER_CERTIFICATE")
ODBC_DRIVER = os.environ.get("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")  # str (nome do driver)
SQL_LOGIN_TIMEOUT = int(os.environ.get("SQL_LOGIN_TIMEOUT", 5)) # conteúdo: int (timeout para conectar)
SQL_QUERY_TIMEOUT   = int(os.environ.get("SQL_QUERY_TIMEOUT", 60)) # conteúdo: int (timeout por SELECT)
SQL_BATCH_SIZE = int(os.environ.get("SQL_BATCH_SIZE", 500))  # conteúdo: int (linhas por chunk)
# Novas configurações globais de retry/backoff:
MAX_RETRIES = int(os.environ.get("SQL_MAX_RETRIES", 5))                 # conteúdo: int (nº máx. de tentativas)
BACKOFF_INITIAL = float(os.environ.get("SQL_BACKOFF_INITIAL", 0.2))     # conteúdo: float (segundos de atraso inicial)
THROTTLE_BETWEEN_CHUNKS = float(os.getenv("SQL_THROTTLE_BETWEEN_CHUNKS", "0.2"))   # conteúdo: segundos
THROTTLE_JITTER = float(os.getenv("SQL_THROTTLE_JITTER", "0.1"))         # conteúdo: segundos
# Autocommit (true/false) - recomendado para CREATE/INSERT/SELECT com #temp_csv
SQL_AUTOCOMMIT = os.getenv("SQL_AUTOCOMMIT", "true").strip().lower() in ("1","true","yes","y","on")
