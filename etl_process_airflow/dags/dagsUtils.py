# s3_utils.py
import io
import os
import pandas as pd
from dbc_reader import DbcReader  # Certifique-se de que essa biblioteca está instalada
import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def processar_dbc_para_csv_local(diretorio_entrada, diretorio_saida, nome_arquivo_saida):
    """
    Lê arquivos .dbc de um diretório local, converte para .csv, mescla os dados e salva como um único arquivo .csv.

    Parâmetros:
    diretorio_entrada (str): Caminho do diretório com os arquivos .dbc.
    diretorio_saida (str): Caminho do diretório onde o arquivo .csv final será salvo.
    nome_arquivo_saida (str): Nome do arquivo .csv final.
    """
    # Garantir que o diretório de saída exista
    if not os.path.exists(diretorio_saida):
        os.makedirs(diretorio_saida)

    # Lista para armazenar DataFrames de todos os arquivos processados
    lista_dfs = []

    # Iterar por todos os arquivos no diretório de entrada
    for arquivo in os.listdir(diretorio_entrada):
        if arquivo.endswith('.dbc'):
            caminho_arquivo = os.path.join(diretorio_entrada, arquivo)
            logging.info(f"Processando arquivo: {caminho_arquivo}")
            try:
                # Ler o arquivo .dbc
                leitor = DbcReader(caminho_arquivo)
                dados = list(leitor)  # Ler os registros

                # Converter para DataFrame do pandas
                df = pd.DataFrame(dados)

                # Adicionar o DataFrame à lista
                lista_dfs.append(df)

                # Salvar cada arquivo individualmente como .csv (opcional)
                caminho_csv_individual = os.path.join(
                    diretorio_saida, arquivo.replace('.dbc', '.csv')
                )
                df.to_csv(caminho_csv_individual, index=False)
                logging.info(f"Arquivo {arquivo} processado e salvo como {caminho_csv_individual}")

            except Exception as e:
                logging.info(f"Erro ao processar o arquivo {arquivo}: {e}")

    # Mesclar todos os DataFrames em um único
    if lista_dfs:
        df_mesclado = pd.concat(lista_dfs, ignore_index=True)
        caminho_csv_final = os.path.join(diretorio_saida, nome_arquivo_saida)
        df_mesclado.to_csv(caminho_csv_final, index=False)
        logging.info(f"Arquivos mesclados salvos como {caminho_csv_final}")
    else:
        logging.info("Nenhum arquivo .dbc foi processado.")
        
def transformar_csv(caminho_entrada, diretorio_saida, nome_arquivo_saida):
    """
    Lê um arquivo CSV, aplica transformações e salva o resultado em um novo arquivo CSV.

    Parâmetros:
    caminho_entrada (str): Caminho do arquivo CSV de entrada.
    diretorio_saida (str): Caminho do diretório onde o arquivo transformado será salvo.
    nome_arquivo_saida (str): Nome do arquivo CSV de saída.
    """
    try:
        # Ler o arquivo CSV de entrada
        logging.info(f"Lendo o arquivo: {caminho_entrada}")
        df = pd.read_csv(caminho_entrada)
        
        df['MES_DIAGN'] = df['ANOMES_DIA'].astype(str).str[-2:].astype(int)

        # Remover a coluna antiga
        df.drop(columns=['ANOMES_DIA'], inplace=True)
        
        # Substituir NaN por "NÃO REGISTRADO" e converter números para inteiros
        df['ANO_TRATAM'] = df['ANO_TRATAM'].apply(lambda x: 'DESCONHECIDO' if pd.isna(x) else int(x))
        
        # Criar a coluna MES_TRA com tratamento
        df['MES_TRATAM'] = df['ANOMES_TRA'].apply(
            lambda x: 'DESCONHECIDO' if pd.isna(x) else int(str(int(x))[-2:])
        )
        
        # Tratar a coluna UF_TRATAM
        df['UF_TRATAM'] = df['UF_TRATAM'].apply(lambda x: 'DESCONHECIDO' if pd.isna(x) else int(x))

        df['MUN_TRATAM'] = df['MUN_TRATAM'].apply(lambda x: 'DESCONHECIDO' if pd.isna(x) else int(x))
        
        # Renomear as colunas para um padrão
        df.rename(columns={'UF_DIAG': 'UF_DIAGN'}, inplace=True)

        df.rename(columns={'MUN_DIAG': 'MUN_DIAGN'}, inplace=True)

        df.rename(columns={'CNES_DIAG': 'CNES_DIAGN'}, inplace=True)

        df.rename(columns={'CNES_TRAT': 'CNES_TRATAM'}, inplace=True)

        df.rename(columns={'TEMPO_TRAT': 'TEMPO_TRATAM'}, inplace=True)

        df.rename(columns={'DT_DIAG': 'DT_DIAGN'}, inplace=True)

        df.rename(columns={'DT_TRAT': 'DT_TRATAM'}, inplace=True)

        # Remover a coluna antiga
        df.drop(columns=['ANOMES_TRA'], inplace=True)
        
        # Substituir os valores 999 por 'NÃO REGISTRADO' na coluna IDADE
        df['IDADE'] = df['IDADE'].replace(999, 'IGNORADA')
        
        # Substituir os valores nulos por 9 na coluna ESTADIAM
        df['ESTADIAM'] = df['ESTADIAM'].fillna(9)
        
        df['ESTADIAM'] = df['ESTADIAM'].astype(int)
        
        # Substituir os valores nulos (NaN) por 0 ou outro valor desejado
        df['CNES_TRATAM'] = df['CNES_TRATAM'].fillna(0)

        # Converter os valores da coluna CNES_TRATAM para inteiro
        df['CNES_TRATAM'] = df['CNES_TRATAM'].astype(int)
        
        # Substituir os valores 0 por 'DESCONHECIDO' na coluna CNES_TRATAM
        df['CNES_TRATAM'] = df['CNES_TRATAM'].replace(0, 'DESCONHECIDO')
        
        df['TEMPO_TRATAM'] = df['TEMPO_TRATAM'].fillna(99999).astype(int)
        
        df.drop(columns={'CNS_PAC'}, inplace=True, axis=1)
        
        # Substituir os valores nulos (NaN) por 0 ou outro valor desejado
        df['DT_TRATAM'] = df['DT_TRATAM'].fillna('DESCONHECIDO')
        
        #Definir as colunas que deseja mover e o ponto de inserção
        colunas_para_mover = ['MES_DIAGN']
        ponto_insercao = 1  # Índice onde as colunas serão inseridas (após 'COL2')

        # Reorganizar as colunas
        colunas = (
            list(df.columns[:ponto_insercao]) +
            colunas_para_mover +
            [col for col in df.columns if col not in colunas_para_mover and col not in df.columns[:ponto_insercao]]
        )
        df = df[colunas]
        
        #Definir as colunas que deseja mover e o ponto de inserção
        colunas_para_mover = ['MES_TRATAM']
        ponto_insercao = 3  # Índice onde as colunas serão inseridas (após 'COL2')

        # Reorganizar as colunas
        colunas = (
            list(df.columns[:ponto_insercao]) +
            colunas_para_mover +
            [col for col in df.columns if col not in colunas_para_mover and col not in df.columns[:ponto_insercao]]
        )
        df = df[colunas]
        
        # Criar o diretório de saída, se necessário
        if not os.path.exists(diretorio_saida):
            os.makedirs(diretorio_saida)

        # Salvar o arquivo transformado
        caminho_saida = os.path.join(diretorio_saida, nome_arquivo_saida)
        df.to_csv(caminho_saida, index=False)
        logging.info(f"Arquivo transformado salvo em: {caminho_saida}")

    except Exception as e:
        logging.info(f"Erro ao transformar o arquivo: {e}")
        
def enviar_csv_para_s3_com_airflow(caminho_arquivo, bucket_name, s3_key, aws_conn_id='aws_default'):
    """
    Envia um arquivo CSV local para um bucket no S3 usando a conexão configurada no Airflow.

    Parâmetros:
    caminho_arquivo (str): Caminho local do arquivo CSV.
    bucket_name (str): Nome do bucket no S3.
    s3_key (str): Caminho do arquivo dentro do bucket no S3.
    aws_conn_id (str): ID da conexão AWS configurada no Airflow. Default é 'aws_default'.
    """
    try:
        # Verificar se o arquivo existe
        if not os.path.exists(caminho_arquivo):
            raise FileNotFoundError(f"O arquivo {caminho_arquivo} não foi encontrado.")

        # Inicializar o S3Hook com a conexão configurada no Airflow
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)

        # Enviar o arquivo para o S3
        logging.info(f"Enviando {caminho_arquivo} para o bucket {bucket_name} com a key {s3_key}...")
        s3_hook.load_file(
            filename=caminho_arquivo,
            bucket_name=bucket_name,
            key=s3_key,
            replace=True  # Substituir o arquivo no S3, se já existir
        )
        logging.info(f"Arquivo enviado com sucesso para s3://{bucket_name}/{s3_key}")

    except Exception as e:
        logging.info(f"Erro ao enviar o arquivo para o S3: {e}")