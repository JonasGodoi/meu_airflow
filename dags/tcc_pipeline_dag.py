from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

CAMINHO_DOS_SCRIPTS = "/opt/airflow/pipelines"

with DAG(
    dag_id="pipeline_dados_transito_tcc",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2025, 9, 4, tz="America/Sao_Paulo"),
    catchup=False,
    doc_md="""### Pipeline de Dados de Trânsito (TCC)""",
    tags=["tcc", "ETL"],
) as dag:
    task_processar_sigesguarda = BashOperator(
        task_id="processar_sigesguarda",
        bash_command=(
            "pip install pandas sqlalchemy mysql-connector-python && "
            f"python {CAMINHO_DOS_SCRIPTS}/sigesguarda.py"
        ),
    )
    task_processar_ippuc = BashOperator(
        task_id="processar_ippuc",
        bash_command=(
            "pip install pandas geopandas sqlalchemy mysql-connector-python && "
            f"python {CAMINHO_DOS_SCRIPTS}/ippuc.py"
        ),
    )