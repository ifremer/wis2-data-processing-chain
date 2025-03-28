import os
import json
import logging
from datetime import datetime
import paho.mqtt.client as mqtt
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.api.common.trigger_dag import trigger_dag

# 📌 Récupérer la variable Airflow
# Paramètres du broker
MQTT_BROKER = Variable.get("MQTT_BROKER_DOMAIN", default_var="broker")
MQTT_PORT = Variable.get("MQTT_BROKER_PORT", default_var=8081)
SSL_ENABLED = Variable.get("MQTT_BORKER_SSL_ENABLED", default_var=False)

# Récupération des identifiants
MQTT_USERNAME = Variable.get("MQTT_ARGO_USERNAME", default_var="prod-files-ro")
MQTT_PASSWORD = Variable.get("MQTT_ARGO_ASSWORD", default_var="prod-files-ro")

MQTT_TOPIC = "diffusion/files/coriolis/argo/#"


def listen_mqtt():
    """Écoute MQTT en continu et déclenche `process_message_dag` pour chaque message reçu."""

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.info("✅ Connexion réussie au broker MQTT")
            client.subscribe(MQTT_TOPIC)
        else:
            logging.error(f"❌ Échec de connexion, code {rc}")

    def on_message(client, userdata, message):
        """Callback exécuté lorsqu'un message MQTT est reçu."""
        try:
            payload = message.payload.decode("utf-8")
            data = json.loads(payload)
            logging.info(f"📩 Message reçu : {data}")

            # Déclenche `process_message_dag` avec les données du message
            trigger_dag(
                dag_id="wis2-publish-message-notification",
                conf=data,  # Envoie le message en paramètre
                replace_microseconds=False,
            )

        except json.JSONDecodeError:
            logging.error("❌ Erreur de parsing du message MQTT")

    # Création du client MQTT
    client = mqtt.Client(transport="websockets")
    if SSL_ENABLED:
        client.tls_set()

    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()  # Écoute en continu


# Définition du DAG principal (écoute MQTT)
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
        "retries": 3,
    },
    description="Écoute les messages MQTT correspondant aux evènements de production de fichiers, et déclenche un DAG pour chaque message reçu.",
    schedule_interval=None,  # Permet au DAG de tourner en continu
    catchup=False,
)


# Opérateur Python pour écouter MQTT
mqtt_listener = PythonOperator(
    task_id="mqtt_listener",
    python_callable=listen_mqtt,
    dag=mqtt_listener_dag,
)
