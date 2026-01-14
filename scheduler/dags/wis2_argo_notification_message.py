"""
Generates, validates, and publishes a WIS2 notification message for a single
diffusion event. The DAG builds a message payload, runs multiple WIS2
validation steps inside Singularity containers (schema, message, data, ETS, KPI),
publishes the final message to the MQTT broker, and cleans up run-scoped
artifacts. Triggered exclusively by the processor DAG as part of the
Argo/WIS2 dissemination pipeline.
"""

import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.singularity.operators.singularity import SingularityOperator
from mqtt.operators.mqtt_pub_sub_container_operator import MqttPubSubContainerOperator

logger = logging.getLogger(__name__)

HOST_DAG_STORE = "/opt/airflow/data/message/{{ dag.dag_id }}"
RUN_ID_DIRECTORY = "{{ dag_run.run_id | replace(':','_') | replace('.','_') }}"
HOST_MESSAGE_STORE = (
    f"{HOST_DAG_STORE}/{RUN_ID_DIRECTORY}"
)
CONTAINER_MESSAGE_STORE = "/data"

# Define DAG to process a WIS2 notification message
# trigger by : wis2-listener-production-file
process_message_dag = DAG(
    dag_id="wis2-publish-message-notification",
    dag_display_name="🔔 WIS2 - Argo notification message",
    default_args={
        "owner": "lbruvryl",
        "email": ["lbruvryl@ifremer.fr"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 5,
    },
    description="Publish notification messages to WMO Information System (WIS2) on MQTT broker.",
    schedule=None,  # Permet au DAG de tourner en continu
    catchup=False,
    is_paused_upon_creation=False,  # Active le DAG au lancement d'Airflow
)

create_host_message_dir = BashOperator(
    task_id="create_host_message_dir_task",
    bash_command=f"mkdir -p {HOST_MESSAGE_STORE}",
)

# Operator dedicated to create WIS2 notification file
generate_notification_message_task = SingularityOperator(
    task_id="generate_notification_message_task",
    image="docker://gitlab-registry.ifremer.fr/amrit/development/wis2-data-processing-chain:latest",
    # image="docker://wis2-data-processing-chain:latest", TODO : must try fully local images
    command="python3 /app/wis2_data_processing_chain/notifications/generate_notification_message.py",
    force_pull=False,
    environment={
        "TASK_ID": "{{ task.task_id }}",
        "MESSAGE": "{{ dag_run.conf.message }}",
        "MESSAGE_STORE_DIR": f"{CONTAINER_MESSAGE_STORE}",
        "APPTAINER_BIND": f"{HOST_MESSAGE_STORE}:{CONTAINER_MESSAGE_STORE}:rw",
        "APPTAINER_DOCKER_USERNAME": "{{ var.value.project_registry_user }}",
        "APPTAINER_DOCKER_PASSWORD": "{{ var.value.project_registry_password }}",
    },
    dag=process_message_dag,
)

# Operator to validate WIS2 notification message
sync_notification_message_schema_task = SingularityOperator(
    task_id="sync_notification_message_schema_task_task",
    image="docker://gitlab-registry.ifremer.fr/amrit/development/wis2-data-processing-chain:latest",
    command="pywis-pubsub schema sync --verbosity DEBUG",
    force_pull=False,
    environment={
        "APPTAINER_DOCKER_USERNAME": "{{ var.value.project_registry_user }}",
        "APPTAINER_DOCKER_PASSWORD": "{{ var.value.project_registry_password }}",
    },
    dag=process_message_dag,
)

# Operator to validate WIS2 notification message
validate_notification_message_task = SingularityOperator(
    task_id="validate_notification_message_task",
    image="docker://gitlab-registry.ifremer.fr/amrit/development/wis2-data-processing-chain:latest",
    command=f"pywis-pubsub message validate {CONTAINER_MESSAGE_STORE}/generate_notification_message_task.json --verbosity DEBUG",
    force_pull=False,
    environment={
        "APPTAINER_BIND": f"{HOST_MESSAGE_STORE}:{CONTAINER_MESSAGE_STORE}:rw",
        "APPTAINER_DOCKER_USERNAME": "{{ var.value.project_registry_user }}",
        "APPTAINER_DOCKER_PASSWORD": "{{ var.value.project_registry_password }}",
    },
    dag=process_message_dag,
)

# Operator to validate data (link, size, checksum) from WIS2 notification message
validate_notification_message_data_task = SingularityOperator(
    task_id="validate_notification_message_data_task",
    image="docker://gitlab-registry.ifremer.fr/amrit/development/wis2-data-processing-chain:latest",
    command=f"pywis-pubsub message verify {CONTAINER_MESSAGE_STORE}/generate_notification_message_task.json --verbosity DEBUG",
    force_pull=False,
    environment={
        "APPTAINER_BIND": f"{HOST_MESSAGE_STORE}:{CONTAINER_MESSAGE_STORE}:rw",
        "APPTAINER_DOCKER_USERNAME": "{{ var.value.project_registry_user }}",
        "APPTAINER_DOCKER_PASSWORD": "{{ var.value.project_registry_password }}",
    },
    dag=process_message_dag,
)

# Operator to validate WIS2 Notification Message Format (WNM)
validate_wnm_data_task = SingularityOperator(
    task_id="validate_wnm_data_task",
    image="docker://gitlab-registry.ifremer.fr/amrit/development/wis2-data-processing-chain:latest",
    command=f"pywis-pubsub ets validate {CONTAINER_MESSAGE_STORE}/generate_notification_message_task.json --verbosity DEBUG",
    force_pull=False,
    environment={
        "APPTAINER_BIND": f"{HOST_MESSAGE_STORE}:{CONTAINER_MESSAGE_STORE}:rw",
        "APPTAINER_DOCKER_USERNAME": "{{ var.value.project_registry_user }}",
        "APPTAINER_DOCKER_PASSWORD": "{{ var.value.project_registry_password }}",
    },
    dag=process_message_dag,
)

# Operator to get key performance indicators
validate_key_performance_indicators_task = SingularityOperator(
    task_id="validate_key_performance_indicators_task",
    image="docker://gitlab-registry.ifremer.fr/amrit/development/wis2-data-processing-chain:latest",
    command=f"pywis-pubsub kpi validate {CONTAINER_MESSAGE_STORE}/generate_notification_message_task.json --verbosity DEBUG",
    force_pull=False,
    environment={
        "APPTAINER_BIND": f"{HOST_MESSAGE_STORE}:{CONTAINER_MESSAGE_STORE}:rw",
        "APPTAINER_DOCKER_USERNAME": "{{ var.value.project_registry_user }}",
        "APPTAINER_DOCKER_PASSWORD": "{{ var.value.project_registry_password }}",
    },
    dag=process_message_dag,
)


# Custom Operator dedicated to publish notification on broker
pub_notification_message_task = MqttPubSubContainerOperator(
    task_id="pub_notification_message_task",
    mqtt_conn_id="mqtt_notification_message",
    topic="origin/a/wis2/fr-ifremer-argo/core/data/ocean/surface-based-observations/drifting-ocean-profilers",
    message_file=f"{CONTAINER_MESSAGE_STORE}/generate_notification_message_task.json",
    bind_directory=f"{HOST_MESSAGE_STORE}:{CONTAINER_MESSAGE_STORE}:rw",
    force_pull=False,
    dag=process_message_dag,
)

# Operator dedicated to cleanup files dedicated to this Dag on success
cleanup_message_storage_task = SingularityOperator(
    task_id="cleanup_message_storage_task",
    image="docker://gitlab-registry.ifremer.fr/amrit/development/wis2-data-processing-chain:latest",
    command="python3 /app/wis2_data_processing_chain/utils/cleanup_data_store.py",
    force_pull=False,
    environment={
        "APPTAINER_BIND": f"{HOST_DAG_STORE}:{CONTAINER_MESSAGE_STORE}:rw",
        "RUN_ID": f"{RUN_ID_DIRECTORY}",
        "APPTAINER_DOCKER_USERNAME": "{{ var.value.project_registry_user }}",
        "APPTAINER_DOCKER_PASSWORD": "{{ var.value.project_registry_password }}",
    },
    dag=process_message_dag,
)

_ = (
    create_host_message_dir
    >> generate_notification_message_task
    >> sync_notification_message_schema_task
    >> validate_notification_message_task
    >> validate_notification_message_data_task
    >> validate_wnm_data_task
    >> validate_key_performance_indicators_task
    >> pub_notification_message_task
    >> cleanup_message_storage_task
)
