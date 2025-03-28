import logging
import os
import json
import uuid
import hashlib
import time
from datetime import datetime, timezone
from urllib.parse import urlparse
import paho.mqtt.client as mqtt
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


def compute_sha512(file_path):
    """Calcule le hash SHA-512 d'un fichier."""
    sha512 = hashlib.sha512()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha512.update(chunk)
    return sha512.hexdigest()


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


def generate_notification_message_from_bufr(
    file_path, base_url, depth=2, output_file=None
):
    """Génère un message JSON basé sur un fichier.

    - Si `output_file` est spécifié, écrit le JSON dans ce fichier.
    - Sinon, retourne une chaîne JSON.
    """

    file_path_id = get_file_path_id(file_path, depth)
    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    file_mod_time = datetime.fromtimestamp(
        os.path.getmtime(file_path), timezone.utc
    ).isoformat()

    sha512_hash = compute_sha512(file_path)

    # Exemple d'ID et d'URN, à adapter selon tes besoins
    data_id = f"wis2/fr-ifremer-argo/core/data/{file_path_id}"
    metadata_id = "urn:wmo:md:fr-ifremer-argo:cor:msg:argo"

    # Génération du message JSON
    message = {
        "id": str(uuid.uuid4()),
        "conformsTo": ["http://wis.wmo.int/spec/wnm/1/conf/core"],
        "type": "Feature",
        "geometry": None,
        "properties": {
            "data_id": data_id,
            "metadata_id": metadata_id,
            "pubtime": datetime.now(timezone.utc).isoformat(),
            "integrity": {"method": "sha512", "value": sha512_hash},
            "datetime": file_mod_time,
        },
        "links": [
            {
                "href": f"{base_url}/{file_path_id}/{file_name}",
                "rel": "canonical",
                "type": "application/bufr",
                "length": file_size,
            }
        ],
    }

    if output_file:
        # Écriture du JSON dans un fichier
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(message, f, indent=4)
        return f"JSON sauvegardé dans {output_file}"
    else:
        # Retourner le JSON sous forme de chaîne
        return json.dumps(message, indent=4)


def generate_notification_message_from_stac(event_message, output_file=None):
    """Génère un message JSON basé sur un fichier.

    - Si `output_file` est spécifié, écrit le JSON dans ce fichier.
    - Sinon, retourne une chaîne JSON.
    """
    # Get data from CloudEvent message
    stac_item = event_message["data"]

    # récupération du premier asset
    first_asset = next(
        iter(stac_item.get("assets", {})), None
    )  # Prend la première clé des assets
    asset = stac_item["assets"][first_asset]

    file_id = get_url_last_n_segments(asset.get("href"), 3)
    # Exemple d'ID et d'URN, à adapter selon tes besoins
    data_id = f"wis2/fr-ifremer-argo/core/data/{file_id}"
    metadata_id = "urn:wmo:md:fr-ifremer-argo:cor:msg:argo"

    # Génération du message JSON
    message = {
        "id": str(uuid.uuid4()),
        "conformsTo": ["http://wis.wmo.int/spec/wnm/1/conf/core"],
        "type": "Feature",
        "geometry": stac_item["geometry"],
        "properties": {
            "data_id": data_id,
            "metadata_id": metadata_id,
            "pubtime": datetime.now(timezone.utc).isoformat(),
            "integrity": {
                "method": "sha512",
                "value": asset.get("file:checksum"),
            },
            "datetime": stac_item["properties"]["datetime"],
        },
        "links": [
            {
                "href": asset.get("href"),
                "rel": "canonical",
                "type": asset.get("type"),
                "length": asset.get("file:size"),
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


def generate_notification_message(**kwargs):
    """Get file creation event and format a WIS2 notification message."""
    message = kwargs["dag_run"].conf

    # validate STAC format with pystac.validation.validate_core
    # https://pystac.readthedocs.io/en/stable/api/validation.html
    # TODO : validate format
    if not message or not isinstance(message, dict) not in message:
        logging.error("❌ Message MQTT invalide ou absent")
        raise ValueError(
            "Le format de l'évènement de production de fichier est invalide."
        )  # 🚨 Lève une exception et stoppe la tâche

    # generation du message de notification
    notification_message_temp_path = os.path.join(
        "/tmp/wis2-publish-message-notification", "test_file.txt"
    )
    notification_message = generate_notification_message_from_stac(
        message, notification_message_temp_path
    )
    logging.info(f"Message de notification WIS2 : ${notification_message}")

    # Ecriture du message dans un fichier
    BASE_PATH = "/tmp/wis2-publish-message-notification"
    os.makedirs(BASE_PATH, exist_ok=True)

    # 📌 Stocker le chemin du fichier dans XCom
    kwargs["ti"].xcom_push(
        key="message_notification_path", value=notification_message_temp_path
    )
    # Stocker la donnée dans XCom
    kwargs["ti"].xcom_push(key="message_notification", value=notification_message)
    logging.info(f"✅ Fichier généré : {notification_message_temp_path}")


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
        logging.error("❌ Erreur : Le message WIS2 est manquant !")
        raise ValueError(
            "Le message de notification WIS2 est invalide ou absent."
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
        "start_date": datetime(2025, 3, 24),
        "retries": 3,
    },
    description="Envoi de messages de notifications MQTT pour WMO Information System (WIS2).",
    schedule_interval=None,  # Permet au DAG de tourner en continu
    catchup=False,
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
    # create_notification_message_task
    generate_notification_message_task
    >> validate_notification_message_task
    # >> validate_notification_message_data_task
    >> validate_wnm_data_task
    >> validate_key_performance_indicators_task
    >> pub_notification_message_task
)
