from airflow import DAG
from airflow.kubernetes.secret import Secret
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

secret_postgres_password_env = Secret(
    # Expose the secret as environment variable.
    deploy_type='env',
    # The name of the environment variable, since deploy_type is `env` rather
    # than `volume`.
    deploy_target='TARGET_POSTGRES_PASSWORD',
    # Name of the Kubernetes Secret
    secret='postgresql-dev',
    # Key of a secret stored in this Secret object
    key='postgres-password'
)

secret_postgres_host_env = Secret(
    # Expose the secret as environment variable.
    deploy_type='env',
    # The name of the environment variable, since deploy_type is `env` rather
    # than `volume`.
    deploy_target='TARGET_POSTGRES_HOST',
    # Name of the Kubernetes Secret
    secret='postgresql-dev-host',
    # Key of a secret stored in this Secret object
    key='host'
)

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
    'kubernetes_spreadsheets_to_postgres', default_args=default_args, schedule_interval=timedelta(days=1))


start = DummyOperator(task_id='start', dag=dag)

extract_load = KubernetesPodOperator(namespace='airflow',
                          image="meltano-project:dev",
                          arguments=["elt", "tap-spreadsheets-anywhere", "target-postgres", "--transform=skip"],
                          # env_vars={"TARGET_POSTGRES_PASSWORD":"mysecretadminpassword"},
                          secrets=[secret_postgres_password_env, secret_postgres_host_env],
                          name="spreadsheets-to-postgres",
                          task_id="spreadsheets-to-postgres-task",
                          get_logs=True,
                          hostnetwork=True,
                          dag=dag
                          )

transform = KubernetesPodOperator(namespace='airflow',
                          image="dbt-project:dev",
                          arguments=["build"],
                          secrets=[secret_postgres_password_env, secret_postgres_host_env],
                          name="transform-postgres",
                          task_id="transform-postgres-task",
                          get_logs=True,
                          hostnetwork=True,
                          dag=dag
                          )

end = DummyOperator(task_id='end', dag=dag)

start > extract_load > transform > end
