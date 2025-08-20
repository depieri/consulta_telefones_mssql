from db_connection import get_connection
import pyodbc

QUERY = """
SELECT 
    t.DDD, t.TELEFONE
FROM CONTATOS c
JOIN HISTORICO_ENDERECOS e ON c.CONTATOS_ID = e.CONTATOS_ID
JOIN HISTORICO_TELEFONES t ON c.CONTATOS_ID = t.CONTATOS_ID
WHERE 
    CONVERT(DATE, c.NASC) = ?
    AND e.CIDADE = ?
    AND e.UF = ?
    AND t.TIPO_TELEFONE = 3
"""

def consultar_telefones(data_nasc, cidade, uf):
    conn = get_connection()
    if conn is None:
        raise Exception("Erro de conexão")

    try:
        cursor = conn.cursor()
        cursor.execute("SET LOCK_TIMEOUT 5000")
        cursor.execute(QUERY, data_nasc, cidade, uf)
        resultados = cursor.fetchall()
        return [f"55{r.DDD}{r.TELEFONE}" for r in resultados]
    except pyodbc.OperationalError as e:
        if "1205" in str(e):  # deadlock
            print("Deadlock detectado. Retentando...")
        raise
    finally:
        conn.close()
