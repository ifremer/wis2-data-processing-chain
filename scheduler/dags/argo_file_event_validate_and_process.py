"""
Processes a single diffusion-event message received from the MQTT listener.
For each run, the DAG creates a dedicated working directory, validates the
message inside a Singularity container, and then triggers the WIS2 publication
DAG. This ensures fully isolated, reproducible, and event-driven processing for
each Argo/WIS2 file-diffusion notification.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.singularity.operators.singularity import SingularityOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Dirs (par run)
HOST_DAG_STORE = "/opt/airflow/data/message/{{ dag.dag_id }}"
RUN_ID_DIRECTORY = "{{ dag_run.run_id | replace(':','_') | replace('.','_') }}"
HOST_MESSAGE_STORE = (
    f"{HOST_DAG_STORE}/{RUN_ID_DIRECTORY}"
)
CONTAINER_MESSAGE_STORE = "/data"

processor_dag = DAG(
    dag_id="argo-diffusion-validate-message",
    dag_display_name="✅ Argo - Validate & process diffusion event message",
    default_args={
        "owner": "lbruvryl",
        "depends_on_past": False,
        "email": ["lbruvryl@ifremer.fr"],
        "email_on_failure": False,
        "email_on_retry": False,
        "start_date": datetime(2025, 3, 24),
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
    },
    description="Validates a single MQTT message, then triggers the publish DAG.",
    schedule=None,  # déclenché par le listener
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=50,  # traite des rafales en parallèle si tu veux
    tags=["MQTT", "Argo", "WIS2"],
)

# 1) Create host dir
create_host_message_dir_task = BashOperator(
    task_id="create_host_message_dir_task",
    bash_command=f"mkdir -p {HOST_MESSAGE_STORE}",
    dag=processor_dag,
)

# 2) Validate (message vient du conf du run)
validate_event_message_data_task = SingularityOperator(
    task_id="validate_event_message_data_task",
    image="docker://gitlab-registry.ifremer.fr/amrit/development/wis2-data-processing-chain:latest",
    # image="docker://wis2-data-processing-chain:latest", TODO : must try fully local images
    command="python3 /app/wis2_data_processing_chain/standards/validate_stac.py",
    force_pull=False,
    environment={
        "TASK_ID": "{{ task.task_id }}",
        # message passé par le listener :
        "MESSAGE": "{{ dag_run.conf.get('message', '') }}",
        "MESSAGE_STORE_DIR": f"{CONTAINER_MESSAGE_STORE}",
        "APPTAINER_BIND": f"{HOST_MESSAGE_STORE}:{CONTAINER_MESSAGE_STORE}:rw",
        "APPTAINER_DOCKER_USERNAME": "{{ var.value.project_registry_user }}",
        "APPTAINER_DOCKER_PASSWORD": "{{ var.value.project_registry_password }}",
    },
    dag=processor_dag,
)

# 3) Trigger processing DAG(s)
trigger_wis2_message_notification_task = TriggerDagRunOperator(
    task_id="trigger_wis2_message_notification_task",
    trigger_dag_id="wis2-publish-message-notification",
    conf={
        "message_event": "{{ dag_run.conf.get('message_event') }}",
        "message": "{{ dag_run.conf.get('message', '') }}",
    },
    dag=processor_dag,
)

# Operator dedicated to cleanup files dedicated to this Dag on success
cleanup_message_storage_task = SingularityOperator(
    task_id="cleanup_message_storage_task",
    image="docker://gitlab-registry.ifremer.fr/amrit/development/wis2-data-processing-chain:1.0.0",
    command="python3 /app/wis2_data_processing_chain/utils/cleanup_data_store.py",
    force_pull=False,
    environment={
        "APPTAINER_BIND": f"{HOST_DAG_STORE}:{CONTAINER_MESSAGE_STORE}:rw",
        "RUN_ID": f"{RUN_ID_DIRECTORY}",
        "APPTAINER_DOCKER_USERNAME": "{{ var.value.wis2_registry_user }}",
        "APPTAINER_DOCKER_PASSWORD": "{{ var.value.wis2_registry_password }}",
    },
    dag=processor_dag,
)


_ = (
    create_host_message_dir_task
    >> validate_event_message_data_task
    >> trigger_wis2_message_notification_task
    >> cleanup_message_storage_task
)
