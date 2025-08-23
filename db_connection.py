import pyodbc
from config import (DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, 
                    ENCRYPT, TRUST_SERVER_CERTIFICATE, ODBC_DRIVER, SQL_LOGIN_TIMEOUT, SQL_AUTOCOMMIT)

def get_connection():
    """
    Cria e retorna uma conexão com o banco de dados MSSQL.
    """
    # Monta a string de conexão para o pyodbc
    try:
        connection_str = (
            f"DRIVER={{{ODBC_DRIVER}}};"  # usa o driver do .env (18)
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
        conn = pyodbc.connect(connection_str, timeout=SQL_LOGIN_TIMEOUT) # timeout de CONEXÃO (login)
        try:
            conn.autocommit = SQL_AUTOCOMMIT   # cada comando se confirma (bom com #temp_csv e fast_executemany)
        except Exception:
            pass
        return conn
    
    except pyodbc.Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None  