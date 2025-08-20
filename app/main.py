import logging
import os
import sys
from datetime import datetime
from app.utils.csv_handler import ler_csv_em_lotes, salvar_csv_resultados
from app.services.telefone_service import consultar_telefones
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
    prefixo = os.path.basename(entrada)[:2]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    saida = f"output/{prefixo}_telefones_{timestamp}.csv"

    logging.info("Iniciando processamento...")
    for chunk in tqdm(ler_csv_em_lotes(entrada)):
        for _, row in chunk.iterrows():
            try:
                telefones = consultar_telefones(row[0], row[1], row[3])
                salvar_csv_resultados(telefones, saida)
            except Exception as e:
                logging.error(f"Erro linha {row.to_dict()}: {e}")

    logging.info(f"Processamento concluído. Resultados salvos em {saida}.")

if __name__ == "__main__":
    main()