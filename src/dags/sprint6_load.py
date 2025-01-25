import logging
import vertica_python

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup


log = logging.getLogger(__name__)

class DAGConfig:
    VERTICA_CONN_NAME = 'VERTICA_ORIGINAL_DWH'

    BATCH_SIZE = 1000

    VERTICA_STG_SCHEMA = 'STV2024111115__STAGING'
    VERTICA_DWH_SCHEMA = 'STV2024111115__DWH'

DC = DAGConfig()

Vertica = BaseHook.get_connection(DC.VERTICA_CONN_NAME)

conn_info = {'host': Vertica.host,
             'port': Vertica.port,
             'user': Vertica.login,     
             'password': Vertica.password,
             'database': 'dwh',
             # Вначале он нам понадобится, а дальше — решите позже сами
            'autocommit': True
}

bash_command_tmpl = """
echo {{ params.files }}
"""

dds_tables = {
        'h_users': """
            SELECT hash(u.id),
                u.id,
                u.REGISTRATION_DT,
                now(),
                '{load_id}'
            FROM {staging}.USERS U
            LEFT JOIN {dwh}.{table} hu ON u.id = hu.user_id
            WHERE hu.user_id IS NULL""",
        'h_groups': """
            SELECT hash(g.id),
                g.id,
                g.REGISTRATION_DT,
                now(),
                '{load_id}'
            FROM {staging}.GROUPS G
            LEFT JOIN {dwh}.{table} hg ON g.id = hg.GROUP_ID
            WHERE hg.GROUP_ID IS NULL""",
        's_auth_history': """
            select uga.hk_l_user_group_activity, 
                gl.user_id_from, 
                gl.event, 
                gl.datetime, 
                now(), 
                '{load_id}' 
            from 
            {staring}.group_log as gl 
            left join {dwh}.h_groups as hg on gl.group_id = hg.group_id 
            left join {dwh}.h_users as hu on gl.user_id = hu.user_id 
            left join {dwh}.l_user_group_activity as uga on hg.hk_group_id = uga.hk_group_id 
                                                                    and  hu.hk_user_id = uga.hk_user_id""",
        'l_user_group_activity': """
                select distinct hash(hk_user_id, hk_group_id) as hk_l_user_group_activity, 
                    hk_user_id, 
                    hk_group_id, 
                    now(), 
                    '{load_id}'
                from {staging}.group_log as sgl 
                left join {dwh}.h_users hu on hu.user_id = sgl.user_id 
                left join {dwh}.h_groups hg on hg.group_id = sgl.group_id 
                where hash(hk_user_id, hk_group_id) not in (
                                    select hk_l_user_group_activity 
                                    from {dwh}.{table}
                    )""",


    }

def insert_dds(log: logging.Logger, querry: str, table:str, conn_info=conn_info, **kwargs):
    insert_expr = f"""
    INSERT INTO {DC.VERTICA_DWH_SCHEMA}.{table} {querry.format(load_id=kwargs['ti'].xcom_pull(key='load_id')
                                                               , staging=DC.VERTICA_STG_SCHEMA
                                                               , dwh=DC.VERTICA_DWH_SCHEMA
                                                               , table=table)}
    """
    log.info(insert_expr)
    with vertica_python.connect(**conn_info) as conn:
        with conn.cursor() as cur:
                cur.execute(insert_expr)
                return cur.fetchone()[0]

def insert_csv(log: logging.Logger, file: str, columns:list, conn_info=conn_info):
    df = pd.read_csv(f'/data/{file}.csv')
    copy_expr = f"""
    COPY {DC.VERTICA_STG_SCHEMA}.{file} ({','.join(columns)}) FROM STDIN DELIMITER ',' ENCLOSED BY '"'
    """
    with vertica_python.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            log.info(len(df))
            for start in range(0, len(df), DC.BATCH_SIZE):
                end = start + DC.BATCH_SIZE
                log.info(f"Loading rows {start}:{end}")
                df.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
                with open('/tmp/chunk.csv', 'rb') as chunk:
                    cur.copy(copy_expr, chunk, buffer_size=65536)
                conn.commit()

def load_from_bucket(log: logging.Logger, file: str) -> None:
    import boto3
    log.info(f"Working on {file}")
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=Variable.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY'),
    )
    s3_client.download_file(
        Bucket='sprint6',
        Key=file,
        Filename=f'/data/{file}'
    ) 

def get_load_id(**kwargs):
    dagrun = kwargs['dag_run']
    kwargs['ti'].xcom_push(key='load_id', value=str(dagrun.start_date).split('.')[0])

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint6'],
    is_paused_upon_creation=False
)
def sprint6_load(**context):
    bucket_files = {'users': ['id', 'chat_name', 'registration_dt', 'country', 'age'], 
                    'groups': ['id', 'admin_id', 'group_name', 'registration_dt', 'is_private'], 
                    'group_log': ['group_id', 'user_id', 'user_id_from', 'group_event',
                           'event_timestamp']}

    prev_tg = None

    load_id = PythonOperator(
        task_id='get_load_id',
        python_callable=get_load_id,
        provide_context=True,
        op_kwargs=None,
    )

    with TaskGroup(group_id='load_from_bucket') as tg:
        for file in bucket_files.keys():
            tmp = PythonOperator(
                task_id=f"s3_{file.replace('.', '_')}",
                python_callable=load_from_bucket,
                op_kwargs={'log': log, 'file': file+'.csv'},
            )
        if prev_tg is None:
            prev_tg = tg
        else:
            prev_tg >> tg
            prev_tg = tg

    with TaskGroup(group_id='insert_into_vertica') as tg:
        for file, columns in bucket_files.items():
            tmp = PythonOperator(
                task_id=f"load_{file}_from_csv",
                python_callable=insert_csv,
                op_kwargs={'log': log, 'file': file, 'columns': columns},
            )
        if prev_tg is None:
            prev_tg = tg
        else:
            prev_tg >> tg
            prev_tg = tg

    prev_tg >> load_id
    prev_tg = load_id
    with TaskGroup(group_id='load_dds') as tg:
        for table, querry in dds_tables.items():
            tmp = PythonOperator(
                task_id=table,
                python_callable=insert_dds,
                op_kwargs={'log': log, 'table': table, 'querry': querry},
                provide_context=True,
            )
        if prev_tg is None:
            prev_tg = tg
        else:
            prev_tg >> tg
            prev_tg = tg


hello_dag = sprint6_load()
