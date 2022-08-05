from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'kubernetes_spreadsheets_to_postgres', default_args=default_args, schedule_interval=timedelta(minutes=10))


start = DummyOperator(task_id='start', dag=dag)

passing = KubernetesPodOperator(namespace='airflow',
                          image="meltano-project:dev",
                          # cmds=["python","-c"],
                          arguments=["elt tap-spreadsheets-anywhere target-postgres --transform=skip"],
                          env_vars={"TARGET_POSTGRES_PASSWORD":"mysecretpassword"},
                          name="spreadsheets-to-postgres",
                          task_id="spreadsheets-to-postgres-task",
                          get_logs=True,
                          dag=dag
                          )


end = DummyOperator(task_id='end', dag=dag)


passing.set_upstream(start)
passing.set_downstream(end)
