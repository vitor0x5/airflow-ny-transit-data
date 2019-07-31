from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import traffic_utils as tfu

default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'start_date': datetime(2019, 6, 10),
   'schedule_interval': timedelta(minutes=120),
   'retries': 0,
   'retry_delay': timedelta(minutes=1)
}

dag = DAG(
   'teste', default_args=default_args, schedule_interval=timedelta(minutes=120)
)

task_import_data = PythonOperator(
   task_id='import_data',
   provide_context=False,
   python_callable=tfu.import_data,
   dag=dag
)

task_pre_proc_calc_medias = PythonOperator(
   task_id='pre_proc_calc_medias',
   python_callable=tfu.pre_proc_calc_medias,
   provide_context=False,
   dag=dag
)

task_pre_proc_clean_data = PythonOperator(
   task_id='pre_proc_clean_data',
   provide_context=False,
   python_callable=tfu.pre_proc_clean_data,
   dag=dag
)

task_pre_proc_merge_new_data = PythonOperator(
   task_id='pre_proc_merge_new_data',
   provide_context=False,
   python_callable=tfu.pre_proc_merge_new_data,
   dag=dag
)

#ordem de execucao
task_import_data >> task_pre_proc_calc_medias >> task_pre_proc_clean_data >> task_pre_proc_merge_new_data
