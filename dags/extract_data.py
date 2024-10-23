from time import sleep
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
from pytrends.request import TrendReq
from io import StringIO
from dateutil.relativedelta import relativedelta


TABLE_NAME = 'test_table'
POSTGRES_CONNECTION = 'postgres_public'


def get_trends_for_region(keywords,
                          cat_code: int,
                          timeframe:str ='today 5-y',
                          region_code:str ='') -> pd.DataFrame():
    """Получает данные о трендах для определенного региона. Если region_code не задан, то поиск по миру
    :param timeframe: Пероид загрузки данных.
    :param keywords: Список ключевых слов.
    :param region_code: Код региона.
    :param cat_code: Категория поиска.
    :return: DataFrame с историческими данными.
    """
    if region_code is None:
        region_code = ''

    pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 25))
    pytrends.build_payload(kw_list=keywords, geo=region_code, cat=cat_code, timeframe=timeframe)

    historical_data = pytrends.interest_over_time()
    historical_data['Region'] = region_code

    return historical_data


def get_all_regions_trends(keywords,
                           cat_code: int,
                           start_date: datetime='2024-01-01',
                           end_date: datetime=datetime.now(),
                           cnt_month: int=6,
                           regions=[]) -> pd.DataFrame():
    """Получает тренды для всех доступных регионов.
    :param keywords: Список ключевых слов.
    :param regions: Список кодов регионов для анализа.
    :param cat_code: Категория поиска.
    :param start_date: Дата начала анализа
    :param end_date: Дата окончания анализа
    :param cnt_month: Количество месяцев в шаге
    :return: Общий DataFrame с данными для всех регионов.
    """
    all_data = pd.DataFrame()

    for region_code in regions:

        current_date = start_date
        while current_date <= end_date:
            timeframe = (f'{current_date.strftime("%Y-%m-%d")} '
                         f'{(current_date + relativedelta(months=cnt_month)).strftime("%Y-%m-%d")}')
            all_data = pd.concat([all_data, get_trends_for_region(keywords=keywords, cat_code=cat_code,
                                                                  timeframe=timeframe, region_code=region_code)])
            current_date = current_date + relativedelta(months=cnt_month)

            sleep(2)

    all_data.reset_index(inplace=True)

    return all_data


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'create_table_and_insert_data',
    default_args=default_args,
    description='Read API and insert data into table',
    schedule_interval=timedelta(days=1),
)


def read_dataframe(**kwargs):
    """Извлекает тренды для заданных регионов и категорий, отправляет его в XCom.
    :param kwargs: Дополнительные аргументы, предоставляемые Airflow.
    """
    keywords = ['Python', 'C++', 'Java', 'C#', 'Go']
    cat_code = 31
    regions = ['RU', 'US']
    start_date =datetime(2024,2, 1)
    end_date = datetime(2024,2, 1)
    cnt_month=5

    df = get_all_regions_trends(keywords=keywords, cat_code=cat_code,
                                start_date=start_date, end_date=end_date,
                                cnt_month=cnt_month, regions=regions)
    if df is None:
        raise ValueError("No data received from previous task")
    df_serialized = df.to_json()
    kwargs['ti'].xcom_push(key='dataframe', value=df_serialized)


def create_table(**kwargs):
    """Создает таблицу в базе данных PostgreSQL на основе структуры DataFrame, полученного через XCom.
    :param kwargs: Дополнительные аргументы, предоставляемые Airflow.
    """
    ti = kwargs['ti']
    df_serialized = ti.xcom_pull(key='dataframe', task_ids='read_dataframe')
    df = pd.read_json(StringIO(df_serialized))

    columns = [f' "{col}" TEXT' for col in df.columns]
    create_table_sql = f"""
    DROP TABLE IF EXISTS {TABLE_NAME};
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        {', '.join(columns)}
    );
    """
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION)
    pg_hook.run(create_table_sql)


def insert_data(**kwargs):
    """Вставляет данные, полученные через XCom, в таблицу PostgreSQL.
    :param kwargs: Дополнительные аргументы, предоставляемые Airflow.
    """
    ti = kwargs['ti']
    df_serialized = ti.xcom_pull(key='dataframe', task_ids='read_dataframe')
    df = pd.read_json(StringIO(df_serialized))

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION)
    engine = pg_hook.get_sqlalchemy_engine()
    df.to_sql(TABLE_NAME, engine, if_exists='append', index=False)


read_df_task = PythonOperator(
    task_id='read_dataframe',
    python_callable=read_dataframe,
    provide_context=True,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    provide_context=True,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    provide_context=True,
    dag=dag,
)

read_df_task >> create_table_task >> insert_data_task
