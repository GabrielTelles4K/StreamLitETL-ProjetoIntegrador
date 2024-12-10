FROM apache/airflow:2.10.2

# Mudar para o usuário root para instalar o pacote
USER airflow

# Instalar o pacote `kaggle`
RUN pip install pandas

RUN pip install dbc_reader

RUN pip install boto3