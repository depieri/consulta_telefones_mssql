import pyodbc
from config import (DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, 
                    ENCRYPT, TRUST_SERVER_CERTIFICATE, SQL_LOGIN_TIMEOUT)

def get_connection():
    """
    Cria e retorna uma conexão com o banco de dados MSSQL.
    """
    # Monta a string de conexão para o pyodbc
    try:
        connection_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={DB_HOST},{DB_PORT};"
            f"DATABASE={DB_NAME};"
            f"UID={DB_USER};"
            f"PWD={DB_PASSWORD};"
            f"Encrypt={ENCRYPT};"
            f"TrustServerCertificate={TRUST_SERVER_CERTIFICATE};"
            f"Pooling=yes;"
            f"Max Pool Size=10;"
            f"APP=DVMsMonitor;"
        )
        conn = pyodbc.connect(connection_str, timeout=SQL_LOGIN_TIMEOUT)
        return conn
    except pyodbc.Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None  