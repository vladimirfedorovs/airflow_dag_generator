import os
import sys
from datetime import datetime, timedelta
from os.path import join
from airflow import DAG
from airflow.models import TaskInstance, DagRun
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import logging
from airflow.providers.oracle.hooks.oracle import OracleHook
from oracledb import DatabaseError
from airflow.models import Variable

# from airflow.providers.oracle.operators.oracle import OracleOperator

logger = logging.getLogger('run_datamart')

os.listdir(pth := '/opt/airflow/custom_data/dm_create/')

sys.path.append(pth)
#импорты самописных модулей проекта, которые не представлены
from run_sql import run_peace_of_code, parse_sql
from datamart import check_dates, delete_dates
from dm_config import Package, dm_config, Table

test_mode = True if Variable.get("DATAMART_TEST_MODE", default_var='0').lower() in ['1', 'true', 'yes'] else False


def run_script(code, ti: TaskInstance):
    oracle_hook_test = OracleHook('ehd_tst')
    dm_date = datetime.strptime(ti.xcom_pull(key="dm_date"), '%Y-%m-%d')
    logger.info(f'Got {dm_date=}')
    run_peace_of_code(code, dm_date, oracle_hook_test.get_sqlalchemy_engine())


def check_date(package: Package, ti: TaskInstance, dag_run: DagRun):
    engs = [OracleHook('ehd_tst').get_sqlalchemy_engine()]
    check_dates(package, engs, 1)
    logger.info(f'{package}')
    delete_dates(package, engs)
    if dm_date_str := dag_run.conf.get('dm_date'):
        dm_date = datetime.strptime(dm_date_str, '%Y-%m-%d')
    else:
        dm_date = next(iter(package.to_calc)) if package.to_calc else None
        dm_date_str = dm_date.strftime("%Y-%m-%d")
    logger.info(
        f'Next date to calc: {dm_date_str}, source = {"config" if dag_run.conf.get("dm_date") else "to_calc"}')
    if dm_date in package.to_calc:
        ti.xcom_push('dm_date', dm_date_str)
        return f'empty'
    else:
        return 'end'


def index_rebuild(t: Table):
    engs = ['ehd_tst_tech']
    for e in engs:
        try:
            with OracleHook(e).get_conn() as conn:
                with conn.cursor() as crs:
                    command = f'ALTER /*+parallel(2)*/ INDEX MARKETING_TECH.{t.name}_cl_dt_pk REBUILD'
                    logger.info(f'Executing {command} on {e}')
                    crs.execute(command)
        except DatabaseError as e:
            logger.error(e)


def get_dag(package: Package):
    # from dm_config import dm_config
    heavy_list = ['ds_dm_card_trans', 'ds_dm_balance']
    if package.name in heavy_list:
        pools = 'datamart_heavy'
    else:
        pools = 'default_pool'

    with DAG(
            f'RUN_DATAMART_{package.name}',
            schedule_interval=None,
            start_date=datetime(2021, 7, 1),
            catchup=False,
            max_active_runs=1,
            default_args={
                "depends_on_past": True,
                'wait_for_downstream': True,
                "retries": 5,
                "retry_delay": timedelta(minutes=5),
                "pool": pools
            },
    ) as dag:
        empty = EmptyOperator(task_id='empty')
        end = EmptyOperator(task_id='end')
        check = BranchPythonOperator(
            task_id='check_date',
            python_callable=check_date,
            op_args=[package]
        )

        index_rebuid_start = [
            PythonOperator(
                task_id=f'alter_index_{t.name}_start',
                python_callable=index_rebuild,
                op_args=[t]
            ) for t in package.tables
        ]

        index_rebuid_end = [
            PythonOperator(
                task_id=f'alter_index_{t.name}_end',
                python_callable=index_rebuild,
                op_args=[t]
            ) for t in package.tables
        ]

        check >> end
        check >> empty >> index_rebuid_start
        tasks = [index_rebuid_start]

        with open(join(pth, 'dm_sql_scripts', f'{package.name}.sql'), encoding='cp1251') as file:
            query = file.read()

        for cnt, code in enumerate(parse_sql(query)):
            if test_mode:
                code = code.replace('/*__comment_for_sample__*/', ' sample(1) ')
            p = PythonOperator(
                task_id=f'{package.name}_{cnt}',
                python_callable=run_script,
                op_args=[code]
            )
            tasks[-1] >> p
            tasks.append(p)
        tasks[-1] >> index_rebuid_end >> end
    return dag


for package in dm_config:
    globals()[f'dag_{package.name}'] = get_dag(package)
