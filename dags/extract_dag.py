from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pytrends.request import TrendReq
import pandas as pd
import time
from sqlalchemy import create_engine


def get_trends_for_region(keywords, cat_code, region_code = ''):
    """
    Получает данные о трендах для определенного региона. Если region_code не задан, то поиск по миру
    :param keywords: Список ключевых слов.
    :param region_code: Код региона.
    :param cat_code: Категория поиска.
    :return: DataFrame с историческими данными.
    """
    pytrends = TrendReq(hl='en-US', tz=360)
    pytrends.build_payload(kw_list=keywords, geo=region_code, cat=cat_code)

    historical_data = pytrends.interest_over_time()

    historical_data['Region'] = region_code
    return historical_data


def get_all_regions_trends(keywords, cat_code, regions):
    """
    Получает тренды для всех доступных регионов.
    :param keywords: Список ключевых слов.
    :param regions: Список кодов регионов для анализа.
    :param cat_code: Категория поиска.
    :return: Общий DataFrame с данными для всех регионов.
    """
    all_data = pd.DataFrame()

    for region_code in regions:
        region_data = get_trends_for_region(keywords, cat_code, region_code)

        if not region_data.empty:
            all_data = pd.concat([all_data, region_data])

        time.sleep(2)

    if regions.__len__() == 0:
        all_data = get_trends_for_region(keywords, cat_code, '')

    return all_data


def get_create_table_sql(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = pg_hook.get_sqlalchemy_engine().connect()

    keywords = ['Python', 'C++', 'Java', 'Kotlin', 'Swift']
    cat_code = 31
    regions = ['RU']

    df = get_all_regions_trends(keywords, cat_code, regions)

    columns = [f"{col} TEXT" for col in df.columns]  # Adjust types as needed
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS your_table_name (
            {', '.join(columns)}
        );
        """
    return create_table_sql

def insert_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='your_postgres_conn_id')
    engine = pg_hook.get_sqlalchemy_engine()

    # Assuming you have a way to get your DataFrame here
    df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})

    # Insert data
    df.to_sql('your_table_name', engine, if_exists='append', index=False)


    # Insert DataFrame to PostgreSQL table
    table_name = 'your_table_name'

    df.to_sql(
        table_name,
        conn,
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )

    conn.close()


default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id=,
    default_args=default_args,
    start_date=datetime(2024, 10, 22),
    schedule_interval='0 0 * * *'
) as dag:
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists dag_runs (
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )

    task2 = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            insert into dag_runs (dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id }}')
        """
    )

    task1 >> task2

    extract_data >> create_table >> insert_data