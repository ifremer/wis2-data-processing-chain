import json
import logging
from datetime import datetime, timedelta

import paho.mqtt.client as mqtt
from cloudevents.http import from_json
from cloudevents.conversion import to_structured
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.api.common.trigger_dag import trigger_dag

# Retrieve MQTT broker configuration from Airflow variables
MQTT_BROKER = Variable.get("MQTT_BROKER_DOMAIN", default_var="broker")
MQTT_PORT = int(Variable.get("MQTT_BROKER_PORT", default_var=8081))
SSL_ENABLED = Variable.get("MQTT_BROKER_SSL_ENABLED", default_var=False) == "true"

# Retrieve MQTT authentication credentials
MQTT_USERNAME = Variable.get("MQTT_ARGO_USERNAME", default_var="prod-files-ro")
MQTT_PASSWORD = Variable.get("MQTT_ARGO_PASSWORD", default_var="prod-files-ro")

# MQTT topic to subscribe to
MQTT_TOPIC = "diffusion/files/coriolis/argo/#"

logger = logging.getLogger(__name__)


def listen_mqtt():
    """
    Continuously listens to MQTT messages and triggers the `process_message_dag` for each received message.
    """

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logger.info("✅ Successfully connected to the MQTT broker")
            try:
                client.subscribe(MQTT_TOPIC)
            except Exception as e:
                logger.error(f"❌ Failed to subscribe to MQTT_TOPIC: {e}", exc_info=True)
                raise
        else:
            logger.error(f"❌ Connection failed with code {rc}", exc_info=True)

    def on_message(client, userdata, message):
        """Callback function triggered when an MQTT message is received."""
        try:
            payload = message.payload.decode("utf-8")
            event = from_json(payload)  # Validate as a CloudEvent
            headers, body = to_structured(event)

            logger.info(
                f"✅ Valid CloudEvent received: {event['type']} from {event['source']}"
            )
            logger.info(f"📩 Message payload: {body}, Headers: {headers}")

            # Trigger the processing DAG with the received event data
            try:
                trigger_dag(
                    dag_id="wis2-publish-message-notification",
                    conf=body,
                    replace_microseconds=False,
                )
                logger.info("🚀 DAG successfully triggered!")
            except Exception as e:
                logger.error(f"❌ Failed to trigger DAG: {e}", exc_info=True)
                raise
        except json.JSONDecodeError as error:
            logger.error(f"❌ Failed to parse MQTT message: {error}", exc_info=True)
            raise

    # Initialize MQTT client
    client = mqtt.Client(transport="websockets")
    if SSL_ENABLED:
        client.tls_set()

    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    # Connect to the broker and start listening
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()


# Define the main DAG for MQTT listening
mqtt_listener_dag = DAG(
    dag_id="wis2-listener-production-file",
    dag_display_name="📂 WIS2 - Ecoute production d'un fichier de données",
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
    description="Écoute les messages MQTT correspondant aux evènements de production de fichiers, et déclenche un DAG pour chaque message reçu.",
    start_date=datetime(2025, 3, 24),
    schedule_interval="@once",
    catchup=False,
    is_paused_upon_creation=False,  # Ensure the DAG is active on Airflow startup
)

# Define the PythonOperator to start MQTT listener
mqtt_listener = PythonOperator(
    task_id="mqtt_listener",
    python_callable=listen_mqtt,
    dag=mqtt_listener_dag,
)
