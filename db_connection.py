import pyodbc
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT, ENCRYPT, TRUST_SERVER_CERTIFICATE

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
        )
        conn = pyodbc.connect(connection_str)
        return conn
    except pyodbc.Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None  