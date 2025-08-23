from db_connection import get_connection
from config import MAX_RETRIES, BACKOFF_INITIAL    # usa a fonte única do config.py
import pyodbc
import time               # conteúdo: sleep do back-off
import logging            # conteúdo: logs estruturados

QUERY = """
    SELECT 
        t.DDD,
        t.TELEFONE
    FROM [dbo].[CONTATOS]            AS c
    JOIN [dbo].[HISTORICO_ENDERECOS] AS e ON e.CONTATOS_ID = c.CONTATOS_ID
    JOIN [dbo].[HISTORICO_TELEFONES] AS t ON t.CONTATOS_ID = c.CONTATOS_ID
    WHERE
        c.NASC >= ? AND c.NASC < DATEADD(DAY, 1, ?)
        AND e.CIDADE = ?
        AND e.UF = ?
        AND t.TIPO_TELEFONE = 3;
"""

def prepare_session(cursor):
    """
    Aplica políticas de sessão para esta conexão.
    cursor: pyodbc.Cursor -> objeto de cursor ativo (retorno: None; efeito na sessão).
    """
    cursor.execute("SET NOCOUNT ON;")                    # reduz chatter TDS (menos overhead de 'N rows affected')
    cursor.execute("SET DEADLOCK_PRIORITY LOW;")         # em disputa, você vira vítima (melhor do que travar outros)
    cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")  # padrão; explícito ajuda auditoria
    cursor.execute("SET QUERY_GOVERNOR_COST_LIMIT 20;")  # limite de custo estimado (~segundos)
    cursor.execute("SET LOCK_TIMEOUT 5000;")             # até 5s aguardando locks

def _is_transient_lock_error(err_msg: str) -> bool:
    """
    Verifica se a mensagem de erro indica deadlock (1205) ou lock timeout (1222),
    ou códigos/termos equivalentes nas mensagens do driver.
    Retorno: bool
    """
    msg = err_msg.lower()
    return (
        "1205" in msg                       # deadlock
        or "1222" in msg                    # lock request timeout
        or "deadlock" in msg                # palavra-chave
        or "sqlstate=40001" in msg          # serialize/deadlock dependendo do driver
        or "hyt00" in msg                   # timeout no ODBC
    )


def execute_with_retries(cursor, query: str, params: tuple = (), is_query: bool = True):
    """
    Executa a query (ou DDL/DML) com retry/backoff para erros transitórios.
    - is_query=True: faz fetchall() e retorna list[Row]
    - is_query=False: apenas executa e retorna None
    """
    attempt = 0                              # conteúdo: contador de tentativas (int)
    while True:                              # efeito: laço até sucesso ou esgotar retries
        try:
            cursor.execute(query, params)    # retorno: None (result set no cursor)
            if is_query:
                rows = cursor.fetchall()         # retorno: list[pyodbc.Row]
                return rows                      # retorno: lista de linhas
            return None
        
        except pyodbc.Error as e:            # captura qualquer erro do ODBC
            err = str(e)
            if _is_transient_lock_error(err) and attempt < MAX_RETRIES:
                delay = BACKOFF_INITIAL * (2 ** attempt)  # conteúdo: atraso em segundos (float)
                logging.warning(
                    f"Lock/Deadlock/Timeout detectado. Tentativa {attempt+1}/{MAX_RETRIES}. "
                    f"Aguardando {delay:.2f}s. Erro: {err}"
                )
                time.sleep(delay)            # efeito: aguarda antes de tentar novamente
                attempt += 1                 # efeito: incrementa tentativa
                continue                     # efeito: tenta de novo
            # não é erro transitório OU esgotaram as tentativas
            logging.error(f"Erro definitivo ao executar query: {err}")
            raise

def executemany_with_retries(cursor, query, params_list):
    attempt = 0
    while True:
        try:
            cursor.executemany(query, params_list)
            return
        except pyodbc.Error as e:
            err = str(e)
            if _is_transient_lock_error(err) and attempt < MAX_RETRIES:
                delay = BACKOFF_INITIAL * (2 ** attempt)
                logging.warning(f"Retrying executemany... Tentativa {attempt+1}/{MAX_RETRIES}")
                time.sleep(delay)
                attempt += 1
            else:
                raise

def consultar_telefones_cursor(cursor, data_nasc: str, cidade: str, uf: str):
    """
    Executa a consulta usando um cursor já preparado pela sessão.
    Parâmetros:
      cursor: pyodbc.Cursor -> cursor ativo da conexão reutilizada (retorno: list[str])
      data_nasc: str (AAAA-MM-DD) -> limiar inferior (>=) e superior aberto (< +1 dia)
      cidade: str -> município (normalizado na planilha)
      uf: str -> UF (normalizado na planilha)
    Retorno:
      list[str] -> cada item no formato '55' + DDD + TELEFONE (ex.: '5511999998888')
    """
    # Executa a query com parâmetros (data_nasc é usada duas vezes)
    rows = execute_with_retries(cursor, QUERY, (data_nasc, data_nasc, cidade, uf))  # retorno: list[Row]
    return [f"55{r.DDD}{r.TELEFONE}" for r in rows]                                  # retorno: list[str]



def consultar_telefones(data_nasc: str, cidade: str, uf: str):
    """
    Wrapper de compatibilidade: abre conexão/cursor, prepara sessão, executa e fecha.
    Útil para chamadas isoladas; no fluxo de produção, prefira consultar_telefones_cursor().
    """
    conn = get_connection()                                     # retorno: pyodbc.Connection
    if conn is None:
        raise Exception("Não foi possível abrir conexão com o MSSQL.")

    try:
        cursor = conn.cursor()                                  # retorno: pyodbc.Cursor
        prepare_session(cursor)                                 # efeito: configura sessão; retorno: None
        return consultar_telefones_cursor(cursor, data_nasc, cidade, uf)
    finally:
        conn.close()  

def consultar_telefones_em_lote_cursor(cursor, registros):
    try:
        # 1) Criação da temp table com retry
        execute_with_retries(cursor, 
            "IF OBJECT_ID('tempdb..#temp_csv') IS NOT NULL DROP TABLE #temp_csv", 
            (), is_query=False
        )
        execute_with_retries(cursor, """
            CREATE TABLE #temp_csv (
                data_nasc DATE NOT NULL,
                cidade VARCHAR(40) NOT NULL,
                uf VARCHAR(2) NOT NULL
            );
        """, (), is_query=False)
        
        # 2) Inserção com retry
        cursor.fast_executemany = True
        executemany_with_retries(cursor,
            "INSERT INTO #temp_csv (data_nasc, cidade, uf) VALUES (?, ?, ?)",
            registros
        )
        
        # 3) Consulta principal
        rows = execute_with_retries(cursor, """
            SELECT DISTINCT t.DDD, t.TELEFONE
            FROM #temp_csv AS csv
            JOIN [dbo].[CONTATOS] AS c ON c.NASC >= csv.data_nasc AND c.NASC < DATEADD(DAY, 1, csv.data_nasc)
            JOIN [dbo].[HISTORICO_ENDERECOS] AS e ON e.CONTATOS_ID = c.CONTATOS_ID AND e.CIDADE = csv.cidade AND e.UF = csv.uf
            JOIN [dbo].[HISTORICO_TELEFONES] AS t ON t.CONTATOS_ID = c.CONTATOS_ID
            WHERE t.TIPO_TELEFONE = 3;
        """, ())
        
        return [f"55{r.DDD}{r.TELEFONE}" for r in rows]
    except Exception as e:
        logging.error(f"Erro durante processamento em lote: {e}")
        return []
    finally:
        # Limpeza da temp table
        try:
            execute_with_retries(cursor,
                "IF OBJECT_ID('tempdb..#temp_csv') IS NOT NULL DROP TABLE #temp_csv",
                (), is_query=False
            )
        except:
            pass
