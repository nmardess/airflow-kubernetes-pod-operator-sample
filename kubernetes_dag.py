from airflow import DAG
from airflow.kubernetes.secret import Secret
from kubernetes.client import models as k8s
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

volume_mount_dbt_docs = k8s.V1VolumeMount(
    name='dbt-docs', mount_path='/dbt/docs', sub_path=None, read_only=True
)

volume_dbt_docs = k8s.V1Volume(
    name='dbt-docs',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='dbt-docs-claim'),
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

dag = DAG( 'kubernetes_analytics', default_args=default_args) #, schedule_interval=timedelta(days=1))


start = DummyOperator(task_id='start', dag=dag)

meltano__extract_load = KubernetesPodOperator(namespace='airflow',
                          image="meltano-project:dev",
                          arguments=["elt", "tap-spreadsheets-anywhere", "target-postgres", "--transform=skip"],
                          secrets=[secret_postgres_password_env, secret_postgres_host_env],
                          name="meltano__extract_load",
                          task_id="meltano__extract_load-task",
                          get_logs=True,
                          #hostnetwork=True,
                          dag=dag
                          )

dbt__run_staging = KubernetesPodOperator(namespace='airflow',
                          image="dbt-project:dev",
                          arguments=["run", "--select", "staging"],
                          secrets=[secret_postgres_password_env, secret_postgres_host_env],
                          name="dbt__run_staging",
                          task_id="dbt__run_staging-task",
                          get_logs=True,
                          #hostnetwork=True,
                          dag=dag
                          )

dbt__run_marts = KubernetesPodOperator(namespace='airflow',
                          image="dbt-project:dev",
                          arguments=["run", "--select", "marts"],
                          secrets=[secret_postgres_password_env, secret_postgres_host_env],
                          name="dbt__run_marts",
                          task_id="dbt__run_marts-task",
                          get_logs=True,
                          #hostnetwork=True,
                          dag=dag
                          )

dbt__generate_docs = KubernetesPodOperator(namespace='airflow',
                          image="dbt-project:dev",
                          arguments=["docs ", "generate"],
                          #secrets=[secret_postgres_password_env, secret_postgres_host_env],
                          name="dbt__generate_docs",
                          task_id="dbt__generate_docs-task",
                          volume_mounts=[volume_mount_dbt_docs],
                          volumes=[volume_dbt_docs],
                          get_logs=True,
                          #hostnetwork=True,
                          dag=dag
                          )

end = DummyOperator(task_id='end', dag=dag)

start >> meltano__extract_load >> dbt__run_staging >> dbt__run_marts >> end
