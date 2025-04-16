import json
import logging
import time

import paho.mqtt.client as mqtt
from cloudevents.http import from_json
from cloudevents.conversion import to_structured
from airflow.models import BaseOperator
from airflow.api.common.trigger_dag import trigger_dag
logger = logging.getLogger(__name__)


class MqttSubOperator(BaseOperator):
    def __init__(
        self,
        topic,
        dag_id_to_trigger,
        mqtt_broker=None,
        mqtt_port=None,
        mqtt_username=None,
        mqtt_password=None,
        ssl_enabled=False,
        *args,
        **kwargs,
    ):

        super().__init__(*args, **kwargs)
        self.topic = topic
        self.dag_id_to_trigger = dag_id_to_trigger
        self.broker = mqtt_broker
        self.port = int(mqtt_port)
        self.username = mqtt_username
        self.password = mqtt_password
        self.ssl_enabled = ssl_enabled

    def execute(self, context):
        """
        Continuously listens to MQTT messages and triggers the `process_message_dag` for each received message.
        """

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                logger.info("✅ Successfully connected to the MQTT broker")
                try:
                    client.subscribe(self.topic)
                except Exception as e:
                    logger.error(
                        f"❌ Failed to subscribe to MQTT_TOPIC: {e}", exc_info=True
                    )
                    raise
            else:
                logger.error(f"❌ Connection failed with code {rc}", exc_info=True)

        def on_disconnect(client, userdata, rc):
            if rc != 0:
                logger.warning(
                    f"⚠️ Disconnected unexpectedly with code {rc}. Reconnecting..."
                )
                try:
                    time.sleep(5)
                    client.reconnect()
                except Exception as e:
                    logger.error(f"❌ Failed to reconnect: {e}")

        def resilient_loop(client):
            while True:
                try:
                    logger.info("🔄 Starting MQTT loop_forever()")
                    client.loop_forever()
                except Exception as e:
                    logger.error(f"❌ MQTT loop failed: {e}")
                    time.sleep(5)
                    try:
                        logger.info("🔁 Attempting to reconnect to MQTT broker...")
                        client.reconnect()
                    except Exception as err:
                        logger.error(f"⛔ Reconnection failed: {err}")
                        time.sleep(30)  # backoff before retry

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
                        dag_id=self.dag_id_to_trigger,
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

        def test_connection(self):
            client = mqtt.Client(transport="websockets")
            client.username_pw_set(self.username, self.password)
            if self.ssl_enabled:
                client.tls_set()
            client.connect(self.broker, self.port, 60)
            client.disconnect()

        # Initialize MQTT client
        client = mqtt.Client(transport="websockets")
        if self.ssl_enabled:
            client.tls_set()

        client.username_pw_set(self.username, self.password)
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect  # 👈 important

        # Connect to the broker and start listening
        try:
            client.connect(self.broker, self.port, 60)
        except Exception as e:
            logger.error(f"❌ Initial connection to broker failed: {e}")
            return

        # Launch loop with resilience
        resilient_loop(client)
