import logging
import time
import os
import sys
from datetime import datetime, date
from app.utils.csv_handler import ler_csv_em_lotes, salvar_csv_resultados
from app.services.telefone_service import prepare_session, consultar_telefones_em_lote_cursor
from db_connection import get_connection
from tqdm import tqdm
from config import SQL_QUERY_TIMEOUT

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

    logging.info("Iniciando processamento...")
    for chunk_idx, chunk in enumerate(
            tqdm(ler_csv_em_lotes(entrada)), # retorno: iterador de DataFrames
            start=1                                            # retorno: chunk_idx começa em 1
        ):
        t0 = time.time()                                       # retorno: float (t0)
        conn = get_connection()                                # retorno: pyodbc.Connection (1 por chunk)
        if conn is None:
            logging.error("Falha ao obter conexão para o chunk; interrompendo.")
            break
        
        linhas = 0                                             # conteúdo: int; garante valor mesmo em exceção
        try:
            cursor = conn.cursor()                             # retorno: pyodbc.Cursor
            cursor.timeout = SQL_QUERY_TIMEOUT                 # efeito: limite por SELECT (s)
            prepare_session(cursor)                            # aplica SETs uma única vez; retorno: None
    
            # Prepara os registros do chunk (data_nasc: date, cidade: str, uf: str)
            registros = []
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
                telefones = consultar_telefones_em_lote_cursor(cursor, registros)  # retorno: list[str]
                salvar_csv_resultados(telefones, saida)        # efeito: grava 1 telefone por linha, sem header
                linhas = len(registros)                        # conteúdo: int (linhas processadas neste chunk)
    
        finally:
            try:
                cursor.close()                                 # encerra cursor do chunk
            except Exception:
                pass
            conn.close()                                       # encerra conexão do chunk

        elapsed = time.time() - t0
        logging.info(
            f"Chunk {chunk_idx} concluído: {linhas} linhas, {elapsed:.2f}s "
            f"({(linhas/elapsed) if elapsed > 0 else 0:.1f} lin/s)"
        )

if __name__ == "__main__":
    main()