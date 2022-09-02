import logging
import json
from copy import copy
from logging import Logger
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable, BaseOperator

from kubernetes.client import models as k8s

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

volume_mount_dbt_target = k8s.V1VolumeMount(
    name='dbt-target', mount_path='/dbt/target', sub_path=None, read_only=False
)

volume_dbt_target = k8s.V1Volume(
    name='dbt-target',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='dbt-target-claim'),
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

class DbtNode:
    def __init__(self, full_name: str, children: List[str], config: Optional[dict]):
        self.full_name = full_name
        self.children = children
        self.is_model = self.full_name.startswith('model')
        self.name = self.full_name.split('.')[-1]
        self.is_persisted = self.is_model and config["materialized"] in ['table', 'incremental', 'view']


class DbtTaskGenerator:

    def __init__(
        self, dag: DAG, manifest: dict
    ) -> None:
        self.dag: DAG = dag
        self.manifest = manifest
        self.persisted_node_map: Dict[str, DbtNode] = self._get_persisted_parent_to_child_map()
        self.logger: Logger = logging.getLogger(__name__)

    def _get_persisted_parent_to_child_map(self) -> Dict[str, DbtNode]:
        node_info = self.manifest['nodes']
        parent_to_child_map = self.manifest['child_map']

        all_nodes: Dict[str, DbtNode] = {
            node_name: DbtNode(
                full_name=node_name,
                children=children,
                config=node_info.get(node_name, {}).get('config')
            )
            for node_name, children in parent_to_child_map.items()
        }

        persisted_nodes = {
            node.full_name: DbtNode(
                full_name=node.full_name,
                children=self._get_persisted_children(node, all_nodes),
                config=node_info.get(node_name, {}).get('config')
            )
            for node_name, node in all_nodes.items()
            if node.is_persisted and node.full_name
        }

        return persisted_nodes

    @classmethod
    def _get_persisted_children(cls, node: DbtNode, all_nodes: Dict[str, DbtNode]) -> List[str]:
        persisted_children = []
        for child_key in node.children:
            child_node = all_nodes[child_key]
            if child_node.is_persisted:
                persisted_children.append(child_key)
            else:
                persisted_children += cls._get_persisted_children(child_node, all_nodes)

        return persisted_children

    def add_all_tasks(self) -> None:
        nodes_to_add: Dict[str, DbtNode] = {}
        for node in self.persisted_node_map:
            included_node = copy(self.persisted_node_map[node])
            included_children = []
            for child in self.persisted_node_map[node].children:
                included_children.append(child)
            included_node.children = included_children
            nodes_to_add[node] = included_node

        self._add_tasks(nodes_to_add)

    def _add_tasks(self, nodes_to_add: Dict[str, DbtNode]) -> None:
        dbt_model_tasks = self._create_dbt_run_model_tasks(nodes_to_add)
        self.logger.info(f'{len(dbt_model_tasks)} tasks created for models')

        for parent_node in nodes_to_add.values():
            if parent_node.is_model:
                self._add_model_dependencies(dbt_model_tasks, parent_node)

    def _create_dbt_run_model_tasks(self, nodes_to_add: Dict[str, DbtNode]) -> Dict[str, BaseOperator]:
        dbt_docker_image_details = Variable.get("docker_dbt-data-platform", deserialize_json=True)
        dbt_model_tasks: Dict[str, BaseOperator] = {
            node.full_name: self._create_dbt_run_task(node.name)
            for node in nodes_to_add.values()
            if node.is_model
        }
        return dbt_model_tasks

    def _create_dbt_run_task(self, model_name: str) -> BaseOperator:
        # This is where you create a task to run the model - see
        # https://docs.getdbt.com/docs/running-a-dbt-project/running-dbt-in-production#using-airflow
        # We pass the run date into our models: f'dbt run --models={model_name} --vars '{"run_date":""}'
        return DummyOperator(dag=self.dag, task_id=model_name, run_date='')

    @staticmethod
    def _add_model_dependencies(dbt_model_tasks: Dict[str, BaseOperator], parent_node: DbtNode) -> None:
        for child_key in parent_node.children:
            child = dbt_model_tasks.get(child_key)
            if child:
                dbt_model_tasks[parent_node.full_name] >> child



dag = DAG( 'kubernetes_analytics', default_args=default_args) 

start = DummyOperator(task_id='start', dag=dag)

meltano__extract_load = KubernetesPodOperator(namespace='airflow',
                          image="meltano-project:dev",
                          arguments=["elt", "tap-spreadsheets-anywhere", "target-postgres", "--transform=skip"],
                          secrets=[secret_postgres_password_env, secret_postgres_host_env],
                          name="meltano__extract_load",
                          task_id="meltano__extract_load-task",
                          get_logs=True,
                          dag=dag
                          )

dbt__generate_manifest = KubernetesPodOperator(namespace='airflow',
                          image="dbt-project:dev",
                          arguments=["compile"],
                          secrets=[secret_postgres_password_env, secret_postgres_host_env],
                          name="dbt__generate_manifest",
                          task_id="dbt__generate_manifest-task",
                          volume_mounts=[volume_mount_dbt_target],
                          volumes=[volume_dbt_target],
                          get_logs=True,
                          dag=dag
                          )

# dbt__run_staging = KubernetesPodOperator(namespace='airflow',
#                           image="dbt-project:dev",
#                           arguments=["run", "--select", "staging"],
#                           secrets=[secret_postgres_password_env, secret_postgres_host_env],
#                           name="dbt__run_staging",
#                           task_id="dbt__run_staging-task",
#                           get_logs=True,
#                           dag=dag
#                           )

# dbt__run_marts = KubernetesPodOperator(namespace='airflow',
#                           image="dbt-project:dev",
#                           arguments=["run", "--select", "marts"],
#                           secrets=[secret_postgres_password_env, secret_postgres_host_env],
#                           name="dbt__run_marts",
#                           task_id="dbt__run_marts-task",
#                           get_logs=True,
#                           dag=dag
#                           )

dbt__generate_docs = KubernetesPodOperator(namespace='airflow',
                          image="dbt-project:dev",
                          arguments=["docs", "generate"],
                          secrets=[secret_postgres_password_env, secret_postgres_host_env],
                          name="dbt__generate_docs",
                          task_id="dbt__generate_docs-task",
                          volume_mounts=[volume_mount_dbt_target],
                          volumes=[volume_dbt_target],
                          get_logs=True,
                          dag=dag
                          )

end = DummyOperator(task_id='end', dag=dag)

with open('/dbt/target/manifest.json', 'r') as json_manifest:
    manifest = json.load(json_manifest)

dbt_task_generator = DbtTaskGenerator(dag, manifest)
dbt_task_generator.add_all_tasks()

start >> meltano__extract_load >> dbt__generate_manifest >> dbt_task_generator >> dbt__generate_docs >> end
