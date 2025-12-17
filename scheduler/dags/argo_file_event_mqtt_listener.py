"""
Event-driven listener DAG that consumes diffusion-event messages from an MQTT
topic and triggers one processor DAG run per message. It maintains a continuous
MQTT session, ensures strict event-level isolation (1 message = 1 run), and
acts as the real-time entry point of the Argo/WIS2 file-diffusion pipeline.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from mqtt.sensors.mqtt_sensor import MqttMessageSensor  # ta classe "stream"

listener_dag = DAG(
    dag_id="argo-diffusion-listener",
    dag_display_name="📂 Argo - Listen Argo file diffusion events",
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
    description="Listen file diffusion events from MQTT broker and fanout to processor (1 message = 1 run).",
    schedule="@once",
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,  # sécurité: un seul TI pour le client_id
    tags=["MQTT", "Argo", "WIS2"],
)

wait_mqtt_msg_task = MqttMessageSensor(
    task_id="wait_mqtt_msg_task",
    mqtt_conn_id="mqtt_file_event_local",
    client_id="argo-mqtt-listener-{{ dag.dag_id }}",
    topic="diffusion/files/coriolis/argo/#",
    qos=1,
    clean_session=False,
    session_expiry=3*24*3600,      # 3j pour prendre en compte les weekends
    retain_handling=1,
    fanout_dag_id="argo-diffusion-validate-message",     # Fanout: 1 run/message vers le processor
    fanout_conf_extra={},           # ajoute des clés si besoin
    airflow_api_conn_id="airflow_api",
    dag=listener_dag,
)
