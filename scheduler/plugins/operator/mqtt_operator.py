import json
import logging
import time

import paho.mqtt.client as mqtt
from cloudevents.http import from_json
from cloudevents.conversion import to_structured
from airflow.models import BaseOperator
from airflow.api.common.trigger_dag import trigger_dag
from typing import Optional


class MqttSubOperator(BaseOperator):
    """
    Airflow operator to subscribe to a MQTT topic and trigger a DAG upon receiving a CloudEvent message.
    """

    def __init__(
        self,
        topic: str,
        dag_id_to_trigger: str,
        mqtt_broker: Optional[str] = None,
        mqtt_port: Optional[int] = 8081,
        mqtt_username: Optional[str] = None,
        mqtt_password: Optional[str] = None,
        ssl_enabled: bool = False,
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
        Entrypoint when the operator is executed within a DAG.
        """

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                self.log.info("✅ Successfully connected to the MQTT broker")
                try:
                    client.subscribe(self.topic)
                    self.log.info(f"📡 Subscribed to topic: {self.topic}")
                except Exception as e:
                    self.log.exception(f"❌ Failed to subscribe to topic: {e}")
                    raise
            else:
                self.log.error(f"❌ Connection failed with code {rc}")

        def on_disconnect(client, userdata, rc):
            if rc != 0:
                self.log.warning(f"⚠️ Disconnected (code {rc}). Attempting reconnect...")
                time.sleep(5)
                try:
                    client.reconnect()
                except Exception as e:
                    self.log.exception("❌ Failed to reconnect to MQTT broker")
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

                self.log.info(f"✅ CloudEvent received: {event['type']} from {event['source']}")
                self.log.debug(f"📩 Payload: {body}")

                try:
                    trigger_dag(
                        dag_id=self.dag_id_to_trigger,
                        conf=body,
                        replace_microseconds=False,
                    )
                    self.log.info(f"🚀 DAG '{self.dag_id_to_trigger}' triggered")
                except Exception as e:
                    self.log.exception("❌ Failed to trigger DAG")
                    raise
            except json.JSONDecodeError as e:
                self.log.exception("❌ Invalid JSON received")
                raise

        def start_mqtt_loop(client):
            """
            Resilient infinite MQTT listening loop.
            """
            while True:
                try:
                    self.log.info("🔄 Starting MQTT client loop_forever()")
                    client.loop_forever()
                except Exception as e:
                    self.log.exception("❌ MQTT loop encountered an error")
                    time.sleep(5)
                    try:
                        self.log.info("🔁 Attempting MQTT reconnection...")
                        client.reconnect()
                    except Exception as reconnect_err:
                        self.log.error(f"⛔ Reconnection failed: {reconnect_err}")
                        time.sleep(30)  # backoff before retry

        # MQTT Client setup
        client = mqtt.Client(transport="websockets")
        if self.ssl_enabled:
            client.tls_set()

        client.username_pw_set(self.username, self.password)
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect

        try:
            self.log.info(f"🔌 Connecting to broker {self.broker}:{self.port}")
            client.connect(self.broker, self.port, 60)
        except Exception as e:
            self.log.exception(f"❌ Initial connection failed {e}")
            raise

        start_mqtt_loop(client)

    def test_connection(self):
        """
        Simple test method to verify connection parameters.
        """
        client = mqtt.Client(transport="websockets")
        if self.ssl_enabled:
            client.tls_set()

        client.username_pw_set(self.username, self.password)
        client.connect(self.broker, self.port, 60)
        client.disconnect()
        self.log.info("✅ MQTT connection test succeeded")
