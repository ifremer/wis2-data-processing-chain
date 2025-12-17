import os
import time
import logging
from cloudevents.http import from_json
from cloudevents.conversion import to_structured
import paho.mqtt.client as mqtt
from airflow.api.common.experimental.trigger_dag import trigger_dag

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOPIC = os.getenv("MQTT_TOPIC")
DAGS_TO_TRIGGER = os.getenv("DAGS_TO_TRIGGER")

BROKER = os.getenv("MQTT_BROKER")
PORT = int(os.getenv("MQTT_PORT", 8081))
USERNAME = os.getenv("MQTT_USERNAME")
PASSWORD = os.getenv("MQTT_PASSWORD")
SSL = os.getenv("MQTT_SSL", "false").lower() == "true"


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("✅ Successfully connected to the MQTT broker")
        try:
            client.subscribe(TOPIC)
            logger.info(f"📡 Subscribed to topic: {TOPIC}")
        except Exception as e:
            logger.exception(f"❌ Failed to subscribe to topic: {e}")
            raise
    else:
        logger.error(f"❌ Connection failed with code {rc}")


def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning(f"⚠️ Disconnected (code {rc}). Attempting reconnect...")
        time.sleep(5)
        try:
            client.reconnect()
        except Exception:
            logger.exception("❌ Failed to reconnect to MQTT broker")
            raise


def on_message(client, userdata, message):
    """
    Callback triggered on message reception.
    Expects valid CloudEvent JSON.
    """
    try:
        payload = message.payload.decode("utf-8")
        event = from_json(payload)
        headers, body = to_structured(event)

        logger.info(
            f"✅ CloudEvent received: {event['type']} from {event['source']} id {event['id']}"
        )
        logger.debug(f"📩 Payload: {body}")

        for dag_id in DAGS_TO_TRIGGER:
            try:
                trigger_dag(
                    dag_id=dag_id,
                    conf=body,
                    replace_microseconds=False,
                )
                logger.info(f"🚀 DAG '{dag_id}' triggered successfully")
            except Exception:
                logger.exception(f"❌ Failed to trigger DAG '{dag_id}'")
                # raise : we do not want to stop broker connection dag is not triggered
    except Exception:
        logger.exception(f"❌ Invalid JSON received : {payload}")
        # raise : we do not want to stop broker connection if message is not valid


def start_mqtt_loop(client):
    """
    Resilient infinite MQTT listening loop.
    """
    while True:
        try:
            logger.info("🔄 Starting MQTT client loop_forever()")
            client.loop_forever()
        except Exception:
            logger.exception("❌ MQTT loop encountered an error")
            time.sleep(5)
            try:
                logger.info("🔁 Attempting MQTT reconnection...")
                client.reconnect()
            except Exception as reconnect_err:
                logger.error(f"⛔ Reconnection failed: {reconnect_err}")
                time.sleep(30)  # backoff before retry


def main():
    client = mqtt.Client(transport="websockets")
    if SSL:
        client.tls_set()

    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER, PORT, 60)
    client.loop_forever()

    # MQTT Client setup
    client = mqtt.Client(transport="websockets")
    if SSL:
        client.tls_set()

    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    try:
        logger.info(f"🔌 Connecting to broker {BROKER}:{PORT}")
        client.connect(BROKER, PORT, 60)
    except Exception as e:
        logger.exception(f"❌ Initial connection failed {e}")
        raise

    start_mqtt_loop(client)


if __name__ == "__main__":
    main()
