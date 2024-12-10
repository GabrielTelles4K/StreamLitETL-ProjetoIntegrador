from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import dagsUtils


def processar_dbc_para_csv():
    
    diretorio_entrada="/data/dados_brutos/"
    
    diretorio_saida="/data/dados_processados_para_csv/"
    
    nome_arquivo_saida="painel_oncologia_2018_a_2023.csv"

    # Chamar a função de conversão
    dagsUtils.processar_dbc_para_csv_local(diretorio_entrada, diretorio_saida, nome_arquivo_saida)
    
def transformar_csv():
    caminho_entrada="/data/dados_processados_para_csv/painel_oncologia_2018_a_2023.csv"
    
    caminho_saida="/data/dados_transformados_finais/"
    
    nome_arquivo_saida="painel_oncologia_2018_a_2023_processado.csv"
    
    dagsUtils.transformar_csv(caminho_entrada,caminho_saida,nome_arquivo_saida)
    
def carga_s3():
    caminho_arquivo="/data/dados_transformados_finais/painel_oncologia_2018_a_2023_processado.csv"
    aws_conn_id = 's3-conn'
    bucket_name = 'etl-oncologia'
    s3_key = 'dados-processados-finais/painel_oncologia_2018_a_2023_processado.csv'
    
    dagsUtils.enviar_csv_para_s3_com_airflow(caminho_arquivo,bucket_name,s3_key,aws_conn_id)

# Definir a DAG
with DAG(
    'etl_streamline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

 
    processar_csv_task = PythonOperator(
        task_id = 'processar_dbc_para_csv',
        python_callable=processar_dbc_para_csv
    )
    
    transformar_csv_task = PythonOperator(
        task_id = 'processar_csv',
        python_callable=transformar_csv
    )
    
    carga_s3_task = PythonOperator(
        task_id = 'carga_s3_task',
        python_callable=carga_s3
    )
    
    processar_csv_task >> transformar_csv_task >> carga_s3_task
