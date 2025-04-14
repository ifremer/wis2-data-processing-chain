import logging
import os
import json
import uuid
import time
from datetime import datetime, timezone
from urllib.parse import urlparse
import base64
from utils.message_store import save_message, cleanup_message_storage
import multihash
import paho.mqtt.client as mqtt
import pystac
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# --------------------------------------------
# Configuration of MQTT broker and credentials
# --------------------------------------------
MQTT_BROKER = Variable.get("MQTT_BROKER_DOMAIN", default_var="broker")
MQTT_PORT = Variable.get("MQTT_BROKER_PORT", default_var=8081)
MQTT_TOPIC = "origin/a/wis2/fr-ifremer-argo/core/data/ocean/surface-based-observations/drifting-ocean-profilers"
SSL_ENABLED = Variable.get("MQTT_BROKER_SSL_ENABLED", default_var=False)
MQTT_USERNAME = Variable.get("MQTT_ARGO_USERNAME", default_var="wis2-argo-rw")
MQTT_PASSWORD = Variable.get("MQTT_ARGO_PASSWORD", default_var="wis2-argo-rw")

logger = logging.getLogger(__name__)


# --------------------------------------
# Utility functions for data processing
# --------------------------------------
def compute_multihash_integrity(multihash_hex):
    """Compute a base64-encoded integrity hash from a multihash value."""
    try:
        hash_bytes = bytes.fromhex(multihash_hex)
        decoded = multihash.decode(hash_bytes)
        hash_method = multihash.constants.CODE_HASHES[decoded.code]
        base64_code = base64.b64encode(decoded.digest).decode()
    except Exception as e:
        logger.error(f"❌ Error when compute multihash integrity : {e}", exc_info=True)
        raise
    return {"method": hash_method, "value": base64_code}


def get_url_last_n_segments(url, n):
    """Extract the last N segments from a given URL."""
    parsed = urlparse(url)
    parts = parsed.path.strip("/").split("/")
    return "/".join(parts[-n:])


def generate_notification_message_from_stac(stac_item_json: dict):
    """Generate a WIS2 notification message from a STAC item."""
    try:
        metadata_id = "urn:wmo:md:fr-ifremer-argo:cor:msg:argo"
        stac_item = pystac.Item.from_dict(stac_item_json)

        for asset_key, asset in stac_item.assets.items():
            logger.info(f"📂 Processing asset: {asset_key}")
            file_id = get_url_last_n_segments(asset.href, 3)
            data_id = f"wis2/fr-ifremer-argo/core/data/{file_id}"
            wis2_integrity = compute_multihash_integrity(
                asset.extra_fields.get("file:checksum")
            )

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
                    "datetime": stac_item.properties.get("datetime"),
                },
                "links": [
                    {
                        "href": asset.href,
                        "rel": "canonical",
                        "type": asset.media_type,
                        "length": asset.extra_fields.get("file:size"),
                    }
                ],
            }

    except Exception as e:
        logger.error(
            f"❌ Error during WIS2 notification message build : {e}", exc_info=True
        )
        raise

    return json.dumps(message, indent=4)


# ----------------------------
# Airflow DAG and task setup
# ----------------------------
def validate_stac_specification(**kwargs):
    """Validate STAC message format."""
    cloudevents_message = kwargs["dag_run"].conf
    dag_id = kwargs["dag"].dag_id
    run_id = kwargs["run_id"]
    task_id = kwargs["task"].task_id

    # Store message in file system in case of error during process
    save_message(dag_id, run_id, task_id, cloudevents_message)

    stac_item_json = cloudevents_message["data"]

    try:
        stac_item = pystac.Item.from_dict(stac_item_json)
        stac_item.validate()
        logger.info("✅ STAC Item is valid!")
    except Exception as e:
        logger.error(f"❌ STAC validation error : {e}", exc_info=True)
        raise

    kwargs["ti"].xcom_push(key="cloudevents_message", value=cloudevents_message)


def generate_notification_message(**kwargs):
    """Generate and store WIS2 notification message."""
    cloudevents_message = kwargs["ti"].xcom_pull(
        task_ids="validate_event_message_data_task", key="cloudevents_message"
    )
    dag_id = kwargs["dag"].dag_id
    run_id = kwargs["run_id"]
    task_id = kwargs["task"].task_id

    stac_item_json = cloudevents_message["data"]

    # save message in a path
    notification_message = generate_notification_message_from_stac(stac_item_json)
    # Store message in file system in case of error during process
    message_path = save_message(dag_id, run_id, task_id, notification_message)

    logger.info("📩 Notification message generated")
    # Store message in XCom
    kwargs["ti"].xcom_push(key="message_notification", value=notification_message)

    # 📌 Stocker le chemin du fichier dans XCom
    kwargs["ti"].xcom_push(key="message_notification_path", value=str(message_path))


def pub_notification_message(**kwargs):
    """Publish notification message to MQTT Broker."""
    notification_message = kwargs["ti"].xcom_pull(
        task_ids="generate_notification_message_task", key="message_notification"
    )

    if not notification_message:
        raise ValueError("Invalid or missing message for MQTT broker !")

    client = mqtt.Client(transport="websockets")
    if SSL_ENABLED:
        client.tls_set()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    try:
        logging.info(f"🔗 Connection to MQTT broker : {MQTT_BROKER}:{MQTT_PORT}...")
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start()
        result, mid = client.publish(MQTT_TOPIC, notification_message)
        if result != mqtt.MQTT_ERR_SUCCESS:
            logger.error(
                f"❌ MQTT publish failed: result={result}, mid={mid}", exc_info=True
            )
            raise RuntimeError("MQTT Publish Error")
        logger.info(f"📤 Message sent → {MQTT_TOPIC} : {notification_message}")
        time.sleep(2)
        client.loop_stop()
        client.disconnect()
        logger.info("🔌 Disconnect from MQTT Broker.")
    except Exception as e:
        logger.error(f"❌ MQTT Connection Error : {e}", exc_info=True)
        raise


def cleanup_on_success(**kwargs):
    dag_id = kwargs["dag"].dag_id
    run_id = kwargs["run_id"]

    if kwargs["dag_run"].get_state() == "success":
        deleted = cleanup_message_storage(dag_id, run_id)
        if deleted:
            logger.info("🧹 Message backup cleaned up successfully.")
        else:
            logger.warning("⚠️ No message backup found to clean.")
    else:
        logger.warning("⚠️ DAG did not complete successfully; cleanup skipped.")


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
        "retries": 5,
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
    dag=process_message_dag,
)

# Operator dedicated to create WIS2 notification file
generate_notification_message_task = PythonOperator(
    task_id="generate_notification_message_task",
    python_callable=generate_notification_message,
    dag=process_message_dag,
)

# Operator to validate WIS2 notification message
validate_notification_message_task = BashOperator(
    task_id="validate_notification_message_task",
    bash_command="cat {{ ti.xcom_pull(task_ids='generate_notification_message_task', key='message_notification_path') }} && pywis-pubsub schema sync && pywis-pubsub message validate {{ ti.xcom_pull(task_ids='generate_notification_message_task', key='message_notification_path') }} --verbosity DEBUG",
    dag=process_message_dag,
)

# Operator to validate data (link, size, checksum) from WIS2 notification message
validate_notification_message_data_task = BashOperator(
    task_id="validate_notification_message_data_task",
    bash_command="pywis-pubsub message verify {{ ti.xcom_pull(task_ids='generate_notification_message_task', key='message_notification_path') }} --verbosity DEBUG",
    dag=process_message_dag,
)

# Operator to validate WIS2 Notification Message Format (WNM)
validate_wnm_data_task = BashOperator(
    task_id="validate_wnm_data_task",
    bash_command="pywis-pubsub ets validate {{ ti.xcom_pull(task_ids='generate_notification_message_task', key='message_notification_path') }}",
    dag=process_message_dag,
)

# Operator to get key performance indicators
validate_key_performance_indicators_task = BashOperator(
    task_id="validate_key_performance_indicators_task",
    bash_command="PYWIS_PUBSUB_GDC_URL=https://api.weather.gc.ca/collections/wis2-discovery-metadata pywis-pubsub kpi validate {{ ti.xcom_pull(task_ids='generate_notification_message_task', key='message_notification_path') }}",
    dag=process_message_dag,
)

# Operator dedicated to publish WIS2 notification
pub_notification_message_task = PythonOperator(
    task_id="pub_notification_message_task",
    python_callable=pub_notification_message,
    dag=process_message_dag,
)

# Operator dedicated to cleanup files dedicated to this Dag on success
cleanup_task = PythonOperator(
    task_id="cleanup_message_storage_task",
    python_callable=cleanup_on_success,
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
    >> cleanup_task
)
