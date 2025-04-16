# dags/dagProductionFileEventListener.py

from datetime import datetime, timedelta
from mqtt_operator import MqttSubOperator
from airflow import DAG
from airflow.models import Variable

# Define the main DAG for MQTT listening
with DAG(
    dag_id="wis2-listener-production-file",
    dag_display_name="📂 WIS2 - Listen file diffusion events",
    default_args={
        "owner": "lbruvryl",
        "depends_on_past": False,
        "email": ["lbruvryl@ifremer.fr"],
        "email_on_failure": False,
        "email_on_retry": False,
        "start_date": datetime(2025, 3, 24),
        "retries": 10,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
    },
    description="Listen file diffusion event from MQTT broker, trigger another DAG uppon message reception.",
    start_date=datetime(2025, 3, 24),
    schedule_interval="@once",
    catchup=False,
    is_paused_upon_creation=False,  # Ensure the DAG is active on Airflow startup

) as dag:

    MqttSubOperator(
        task_id="mqtt_listener",
        topic="diffusion/files/coriolis/argo/#",
        dag_id_to_trigger="wis2-publish-message-notification",
        mqtt_broker=Variable.get("MQTT_BROKER_DOMAIN", default_var="broker"),
        mqtt_port=Variable.get("MQTT_BROKER_PORT", default_var=8081),
        mqtt_username=Variable.get("MQTT_ARGO_USERNAME", default_var="prod-files-rw"),
        mqtt_password=Variable.get("MQTT_ARGO_PASSWORD", default_var="prod-files-rw"),
    )
