import logging
import os
import sys
from datetime import datetime
from app.utils.csv_handler import ler_csv_em_lotes, salvar_csv_resultados
from app.services.telefone_service import prepare_session, consultar_telefones_cursor
from db_connection import get_connection
from tqdm import tqdm

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
    for chunk in tqdm(ler_csv_em_lotes(entrada)):
        conn = get_connection()                                # retorno: pyodbc.Connection (1 por chunk)
        if conn is None:
            logging.error("Falha ao obter conexão para o chunk; interrompendo.")
            break
        
        try:
            cursor = conn.cursor()                             # retorno: pyodbc.Cursor
            prepare_session(cursor)                            # aplica SETs uma única vez; retorno: None
    
            for _, row in chunk.iterrows():
                try:
                    # data_nasc=row[0], cidade=row[1], uf=row[3] (já normalizados na planilha)
                    telefones = consultar_telefones_cursor(cursor, row[0], row[1], row[3])  # retorno: list[str]
                    salvar_csv_resultados(telefones, saida)                                 # efeito: grava linhas no CSV
                except Exception as e:
                    logging.error(f"Erro linha {row.to_dict()}: {e}")
    
        finally:
            try:
                cursor.close()                                 # encerra cursor do chunk
            except Exception:
                pass
            conn.close()                                       # encerra conexão do chunk


if __name__ == "__main__":
    main()