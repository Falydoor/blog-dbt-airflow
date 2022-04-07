from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow_dbt.operators.dbt_operator import DbtRunOperator
from requests import Session
from datetime import datetime
import json
import awswrangler as wr
import pandas as pd


def save_quote():
    # Set headers
    url = "https://sandbox-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol=BTC"
    headers = {
        "Accepts": "application/json",
        "X-CMC_PRO_API_KEY": Variable.get('API_KEY')
    }

    # Get quote
    session = Session()
    session.headers.update(headers)
    data = json.loads(session.get(url).text)

    # Load quote to df
    df = pd.DataFrame(data["data"]["BTC"]["quote"]["USD"], index=['i',])

    # Add audit columns
    df['inserted_at'] = datetime.now()

    # Save quote to Redshift
    con = wr.redshift.connect_temp(cluster_identifier="blog-dbt-airflow", user="awsuser", database="dev", auto_create=False)
    wr.redshift.to_sql(
        df=df,
        table="quote",
        schema="public",
        con=con
    )
    con.close()


with DAG("bitcoin-price", schedule_interval="*/5 * * * *", start_date=datetime(2022, 4, 5), catchup=False) as dag:
    save_quote_task = PythonOperator(task_id="save-quote",
                                     python_callable=save_quote)

    dbt_task = DbtRunOperator(task_id="dbt",
                              dbt_bin="/usr/local/airflow/.local/bin/dbt",
                              profiles_dir="/usr/local/airflow/dags/blog_dbt_airflow/",
                              dir="/usr/local/airflow/dags/blog_dbt_airflow/",
                              models="quote")

    save_quote_task >> dbt_task
