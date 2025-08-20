def ler_csv_em_lotes(caminho, tamanho_lote=500):
    import pandas as pd
    return pd.read_csv(caminho, chunksize=tamanho_lote, dtype=str)

def salvar_csv_resultados(lista_telefones, caminho_saida):
    with open(caminho_saida, 'a') as f:
        for telefone in lista_telefones:
            f.write(f"{telefone}\n")