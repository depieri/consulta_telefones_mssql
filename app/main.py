import random
import logging
import time
import os
import sys
from datetime import datetime, date
from app.utils.csv_handler import ler_csv_em_lotes, salvar_csv_resultados
from app.services.telefone_service import prepare_session, consultar_telefones_em_lote_cursor
from db_connection import get_connection
from tqdm import tqdm
from config import SQL_QUERY_TIMEOUT, THROTTLE_BETWEEN_CHUNKS, THROTTLE_JITTER

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

def main():
    if len(sys.argv) < 2:
        logging.error("Uso: python3 -m app.main <caminho_csv_entrada>")
        sys.exit(1)
    
    entrada = sys.argv[1]
    os.makedirs('output', exist_ok=True)
    prefixo = os.path.basename(entrada)[:2]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    saida = f"output/{prefixo}_telefones_{timestamp}.csv"

    # 1) Abre UMA conexão e prepara sessão
    
    conn = get_connection()
    if conn is None:
        logging.error("Falha ao obter conexão; interrompendo.")
        sys.exit(1)

    cursor = conn.cursor()                             # retorno: pyodbc.Cursor
    cursor.timeout = SQL_QUERY_TIMEOUT                 # efeito: limite por SELECT (s)
    prepare_session(cursor)                            # aplica SETs uma única vez; retorno: None

    logging.info("Iniciando processamento...")

    for chunk_idx, chunk in enumerate(
            tqdm(ler_csv_em_lotes(entrada)), # retorno: iterador de DataFrames
            start=1                                            # retorno: chunk_idx começa em 1
        ):
        t0 = time.time()                                       # retorno: float (t0)
        linhas = 0                                             # conteúdo: int; garante valor mesmo em exceção
        
        registros = []                                         # Prepara os registros do chunk (data_nasc: date, cidade: str, uf: str)

        for _, row in chunk.iterrows():
            try:
                registros.append((                          # retorno: None (append na lista)
                    date.fromisoformat(row[0]),             # conteúdo: datetime.date (YYYY-MM-DD)
                    row[1],                                 # conteúdo: str (cidade)
                    row[3]                                  # conteúdo: str (uf)
                ))
            except Exception as e:
                logging.error(f"Erro ao preparar linha {row.to_dict()}: {e}")

        if registros:                                      # evita SELECT se o chunk vier vazio
            # 2) Executa o chunk com retry de sessão (reconecta se necessário)
            attempts = 0
            while True:
                try:
                    telefones = consultar_telefones_em_lote_cursor(cursor, registros)  # retorno: list[str]
                    salvar_csv_resultados(telefones, saida)        # efeito: grava 1 telefone por linha, sem header
                    linhas = len(registros)                        # conteúdo: int (linhas processadas neste chunk)
                    break  # sucesso no chunk
                except Exception as e:
                    msg = str(e)
                    # Erros que sugerem queda de conexão/session invalid
                    if any(x in msg for x in ("08S01", "08001", "HY000", "Communication link failure")) and attempts < 3:
                        attempts += 1
                        wait = 0.5 * attempts  # backoff simples para reconexão
                        logging.warning(f"Conexão caiu (chunk {chunk_idx}). Tentando reconectar em {wait:.1f}s... Erro: {msg}")
                        time.sleep(wait)
                        # reconecta e re-prepara sessão/cursor
                        try:
                            cursor.close()
                        except Exception:
                            pass
                        try:
                            conn.close()
                        except Exception:
                            pass
                        conn = get_connection()
                        if conn is None:
                            logging.error("Falha ao reconectar.")
                            raise
                        cursor = conn.cursor()
                        cursor.timeout = SQL_QUERY_TIMEOUT
                        prepare_session(cursor)
                        continue
                    # outro erro qualquer: propaga
                    raise
                
        elapsed = time.time() - t0
        logging.info(
            f"Chunk {chunk_idx} concluído: {linhas} linhas, {elapsed:.2f}s "
            f"({(linhas/elapsed) if elapsed > 0 else 0:.1f} lin/s)"
        )
        time.sleep(THROTTLE_BETWEEN_CHUNKS + random.random() * THROTTLE_JITTER)
    # encerra ao final
    try:
        cursor.close()
    except Exception:
        pass
    conn.close()

if __name__ == "__main__":
    main()