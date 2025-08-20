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
SQL_COMMAND_TIMEOUT = int(os.environ.get("SQL_COMMAND_TIMEOUT", 5))