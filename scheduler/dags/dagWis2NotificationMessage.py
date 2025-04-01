import logging
import os
import json
import uuid
import time
from datetime import datetime, timezone
from urllib.parse import urlparse
import base64
import multihash
import paho.mqtt.client as mqtt
import pystac
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# 📌 Récupérer la variable Airflow
# Paramètres du broker
MQTT_BROKER = Variable.get("MQTT_BROKER_DOMAIN", default_var="broker")
MQTT_PORT = Variable.get("MQTT_BROKER_PORT", default_var=8081)
MQTT_TOPIC = "origin/a/wis2/fr-ifremer-argo/core/data/ocean/surface-based-observations/drifting-ocean-profilers"
SSL_ENABLED = Variable.get("MQTT_BORKER_SSL_ENABLED", default_var=False)

# Récupération des identifiants
MQTT_USERNAME = Variable.get("MQTT_ARGO_USERNAME", default_var="wis2-argo-rw")
MQTT_PASSWORD = Variable.get("MQTT_ARGO_ASSWORD", default_var="wis2-argo-rw")

###############################################
# Generation du message de notification WIS2  #
###############################################


def compute_multihash_integrity(multihash_hex):
    hash_bytes = bytes.fromhex(multihash_hex)  # 🔹 Convertir hex en bytes
    decoded = multihash.decode(hash_bytes)
    hash_method = multihash.constants.CODE_HASHES[decoded.code]
    base64_code = base64.b64encode(decoded.digest).decode()
    return {
        "method": hash_method,
        "value": base64_code,
    }


def get_file_path_id(file_path, depth):
    """Construit file_path_id en gardant les N derniers niveaux du chemin."""
    parts = file_path.strip(os.sep).split(os.sep)
    return os.path.join(*parts[-depth:])


def get_url_last_n_segments(url, n):
    """Extrait les N derniers segments d'une URL."""
    parsed = urlparse(url)  # Parse l'URL pour séparer domaine et chemin
    parts = parsed.path.strip("/").split("/")  # Divise le chemin en segments
    last_n = "/".join(
        parts[-n:]
    )  # Construit le nouveau chemin avec N derniers éléments

    return last_n  # Retourne uniquement la partie modifiée


def generate_notification_message_from_stac(stac_item_json, output_file=None):
    """Génère un message JSON basé sur un fichier.

    - Si `output_file` est spécifié, écrit le JSON dans ce fichier.
    - Sinon, retourne une chaîne JSON.
    """
    metadata_id = "urn:wmo:md:fr-ifremer-argo:cor:msg:argo"
    stac_item = pystac.Item.from_dict(stac_item_json)

    # récupération du premier asset ??
    # Boucle sur les assets du STAC Item
    for asset_key, asset in stac_item.assets.items():
        logging.info(f"📂 Asset key: {asset_key}")  # Nom de l'asset
        file_id = get_url_last_n_segments(asset.href, 3)
        # Exemple d'ID et d'URN, à adapter selon tes besoins
        data_id = f"wis2/fr-ifremer-argo/core/data/{file_id}"
        wis2_integrity = compute_multihash_integrity(
            asset.extra_fields.get("file:checksum")
        )

        # Génération du message JSON
        message = {
            "id": str(uuid.uuid4()),
            "conformsTo": ["http://wis.wmo.int/spec/wnm/1/conf/core"],
            "type": "Feature",
            "geometry": stac_item.geometry,
            "properties": {
                "data_id": data_id,
                "metadata_id": metadata_id,
                "pubtime": datetime.now(timezone.utc).isoformat(),
                "integrity": wis2_integrity,
                "datetime": stac_item.properties.get("datetime", None),
            },
            "links": [
                {
                    "href": asset.href,
                    "rel": "canonical",
                    "type": asset.media_type,
                    "length": asset.extra_fields.get("file:size", None),
                }
            ],
        }

    if output_file:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        # Écriture du JSON dans un fichier
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(message, f, indent=4)
        return json.dumps(message, indent=4)
    else:
        # Retourner le JSON sous forme de chaîne
        return json.dumps(message, indent=4)


def validate_stac_specification(**kwargs):
    """Get file creation event and format a WIS2 notification message."""
    cloudevents_message = kwargs["dag_run"].conf

    # Get data from CloudEvent message
    stac_item_json = cloudevents_message["data"]

    try:
        # Charger le JSON en dictionnaire
        # stac_item_dict = json.loads(stac_item_json)
        # Créer un STAC Item avec PySTAC
        stac_item = pystac.Item.from_dict(stac_item_json)

        # Valider l'Item STAC avec les schémas STAC officiels
        stac_item.validate()

        logging.info("✅ STAC Item est valide !")

    except Exception as e:
        logging.error(f"❌ Erreur de validation STAC : {e}")
        return

    kwargs["ti"].xcom_push(key="cloudevents_message", value=cloudevents_message)


def generate_notification_message(**kwargs):
    """Get file creation event and format a WIS2 notification message."""
    # Récupérer du message de notification validé
    cloudevents_message = kwargs["ti"].xcom_pull(
        task_ids="validate_event_message_data_task",
        key="cloudevents_message",
    )

    # Get data from CloudEvent message
    stac_item_json = cloudevents_message["data"]

    # generation du message de notification
    wis_notification_message_temp_path = os.path.join(
        "/tmp/wis2-publish-message-notification", "notification-message.json"
    )
    wis_notification_message = generate_notification_message_from_stac(
        stac_item_json, wis_notification_message_temp_path
    )

    logging.info(f"Message de notification WIS2 : ${wis_notification_message}")

    # Ecriture du message dans un fichier
    BASE_PATH = "/tmp/wis2-publish-message-notification"
    os.makedirs(BASE_PATH, exist_ok=True)

    # 📌 Stocker le chemin du fichier dans XCom
    kwargs["ti"].xcom_push(
        key="message_notification_path", value=wis_notification_message_temp_path
    )
    # Stocker la donnée dans XCom
    kwargs["ti"].xcom_push(key="message_notification", value=wis_notification_message)
    logging.info(f"✅ Fichier généré : {wis_notification_message_temp_path}")


###############################################
# Publication du message de notification WIS2 #
###############################################


# Callback lors de la connexion au broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connexion réussie au broker MQTT")
    else:
        print(f"❌ Échec de connexion, code d'erreur : {rc}")


# Callback lors de la réception d'un message
def on_message(client, userdata, msg):
    print(f"📩 Message reçu sur {msg.topic} : {msg.payload.decode()}")


def pub_notification_message(**kwargs):
    """Publish notification message on the dedicated wis2 topic on MQTT Broker."""
    # Récupérer du message de notification validé
    notification_message = kwargs["ti"].xcom_pull(
        task_ids="generate_notification_message_task",
        key="message_notification",
    )

    # notification_message_file = kwargs["ti"].xcom_pull(
    #     task_ids="generate_notification_message_task", key="message_notification_path"
    # )

    if notification_message is None:
        logging.error(
            "❌ Erreur : Le message envoyer au broker MQTT est invalide ou absent."
        )
        raise ValueError(
            "Le message envoyer au broker MQTT est invalide ou absent."
        )  # 🚨 Lève une exception et stoppe la tâche

    # Création du client MQTT avec WebSockets
    client = mqtt.Client(transport="websockets")

    # Configuration SSL si activé
    if SSL_ENABLED:
        logging.info("✅ SSL activé pour la connexion MQTT")
        client.tls_set()

    # Ajout des identifiants d'authentification
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    # Attacher les callbacks
    client.on_connect = on_connect
    client.on_message = on_message

    # Connexion au broker
    logging.info(f"🔗 Connexion au broker MQTT : {MQTT_BROKER}:{MQTT_PORT}...")
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start()
    except Exception as e:
        logging.info(f"❌ Impossible de se connecter au broker MQTT : {e}")
        return

    # Publier le fichier JSON sous forme de message MQTT
    # message = json.dumps(notification_message, ensure_ascii=False)
    client.publish(MQTT_TOPIC, notification_message)
    logging.info(f"📤 Message envoyé → {MQTT_TOPIC} : {notification_message}")

    # Attendre un court instant pour s'assurer que le message est bien envoyé
    time.sleep(2)

    # Déconnexion propre
    client.loop_stop()
    client.disconnect()
    logging.info("✅ Déconnexion du client MQTT.")


# Define DAG to process a WIS2 notification message
# trigger by : wis2-listener-production-file
process_message_dag = DAG(
    dag_id="wis2-publish-message-notification",
    dag_display_name="🔔 WIS2 - Publication de messages de notifications",
    default_args={
        "owner": "lbruvryl",
        "email": ["lbruvryl@ifremer.fr"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
    },
    description="Envoi de messages de notifications MQTT pour WMO Information System (WIS2).",
    schedule_interval=None,  # Permet au DAG de tourner en continu
    catchup=False,
    is_paused_upon_creation=False,  # Active le DAG au lancement d'Airflow
)

# Operator dedicated to validate STAC specification from event
validate_event_message_data_task = PythonOperator(
    task_id="validate_event_message_data_task",
    python_callable=validate_stac_specification,
    provide_context=True,
    dag=process_message_dag,
)

# Operator dedicated to create WIS2 notification file
generate_notification_message_task = PythonOperator(
    task_id="generate_notification_message_task",
    python_callable=generate_notification_message,
    provide_context=True,
    dag=process_message_dag,
)

validate_notification_message_task = BashOperator(
    task_id="validate_notification_message_task",
    bash_command="pywis-pubsub schema sync && pywis-pubsub message validate {{ ti.xcom_pull(task_ids='generate_notification_message_task', key='message_notification_path') }} --verbosity DEBUG",
    dag=process_message_dag,
)

validate_notification_message_data_task = BashOperator(
    task_id="validate_notification_message_data_task",
    bash_command="pywis-pubsub message verify {{ ti.xcom_pull(task_ids='generate_notification_message_task', key='message_notification_path') }} --verbosity DEBUG",
    dag=process_message_dag,
)

validate_wnm_data_task = BashOperator(
    task_id="validate_wnm_data_task",
    bash_command="pywis-pubsub ets validate {{ ti.xcom_pull(task_ids='generate_notification_message_task', key='message_notification_path') }}",
    dag=process_message_dag,
)

validate_key_performance_indicators_task = BashOperator(
    task_id="validate_key_performance_indicators_task",
    bash_command="PYWIS_PUBSUB_GDC_URL=https://api.weather.gc.ca/collections/wis2-discovery-metadata pywis-pubsub kpi validate {{ ti.xcom_pull(task_ids='generate_notification_message_task', key='message_notification_path') }}",
    dag=process_message_dag,
)

# Operator dedicated to publish WIS2 notification
pub_notification_message_task = PythonOperator(
    task_id="pub_notification_message_task",
    python_callable=pub_notification_message,
    provide_context=True,
    dag=process_message_dag,
)

(
    validate_event_message_data_task
    >> generate_notification_message_task
    >> validate_notification_message_task
    >> validate_notification_message_data_task
    >> validate_wnm_data_task
    >> validate_key_performance_indicators_task
    >> pub_notification_message_task
)
