def ler_csv_em_lotes(caminho, tamanho_lote=None):
    import pandas as pd                                 # conteúdo: módulo pandas
    from config import SQL_BATCH_SIZE                   # conteúdo: int vindo do .env (fallback 500)
    if tamanho_lote is None:                            # efeito: só usa config se ninguém passar tamanho_lote
        tamanho_lote = SQL_BATCH_SIZE                   # conteúdo: int efetivo do chunk size
    return pd.read_csv(caminho,
                        chunksize=tamanho_lote,
                          dtype=str, na_filter=False,
                            usecols=['data_nascimento', 'municipio', 'uf_sigla'])  # retorno: iterador (TextFileReader) de DataFrames

def salvar_csv_resultados(lista_telefones, caminho_saida):
    from pathlib import Path                                   # importa utilitário de paths (retorno: módulo)
    Path(caminho_saida).parent.mkdir(parents=True, exist_ok=True)
    
    if not lista_telefones:                                    # verifica lista vazia/None
        return 0                                               # retorno: int (0 linhas gravadas)
    
    with open(caminho_saida, 'a', encoding="utf-8", newline="\n") as f:
        for telefone in lista_telefones:
            f.write(f"{telefone}\n")
    return sum(1 for t in lista_telefones if t)                # retorno: int (qtd. linhas gravadas)