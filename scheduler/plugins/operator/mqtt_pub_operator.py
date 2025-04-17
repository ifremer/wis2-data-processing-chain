import time
import json
from typing import Optional, Union

import paho.mqtt.client as mqtt
from airflow.models import BaseOperator, Variable
from airflow.utils.context import Context


class MqttPubOperator(BaseOperator):
    """
    Publishes a message (dict or str) to a specified MQTT topic using WebSockets.

    Can be used to publish a WIS2 notification message to a broker.
    """

    def __init__(
        self,
        topic: str,
        message: Union[dict, str],
        mqtt_broker: Optional[str] = None,
        mqtt_port: Optional[int] = None,
        mqtt_username: Optional[str] = None,
        mqtt_password: Optional[str] = None,
        ssl_enabled: Optional[bool] = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.topic = topic
        self.message = message

        self.broker = mqtt_broker
        self.port = int(mqtt_port)
        self.username = mqtt_username
        self.password = mqtt_password
        self.ssl_enabled = ssl_enabled

    def execute(self, context: Context):
        """
        Execute the MQTT publication.

        Args:
            context (Context): Airflow context injected at runtime.
        """

        # Validate and prepare message
        if not self.message:
            raise ValueError("❌ No message found or message is empty.")

        if isinstance(self.message, dict):
            try:
                message_str = json.dumps(self.message)
            except Exception as e:
                raise ValueError("❌ Failed to serialize message to JSON.") from e
        elif isinstance(self.message, str):
            message_str = self.message
        else:
            raise TypeError("❌ Message must be a `dict` or `str`.")

        # Create and configure MQTT client
        client = mqtt.Client(transport="websockets")
        if self.ssl_enabled:
            self.log.info("🔒 SSL enabled for MQTT connection.")
            client.tls_set()

        client.username_pw_set(self.username, self.password)

        try:
            self.log.info(f"🔗 Connecting to MQTT broker: {self.broker}:{self.port}")
            client.connect(self.broker, self.port, 60)
            client.loop_start()

            # Publish message to topic
            result, mid = client.publish(self.topic, message_str)

            if result != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"❌ MQTT publish failed (result={result}, mid={mid})")

            self.log.info(f"📤 Message published to topic '{self.topic}'")
            self.log.debug(f"📝 Payload: {message_str}")

            # Wait to ensure delivery and cleanly disconnect
            time.sleep(2)
            client.loop_stop()
            client.disconnect()
            self.log.info("🔌 MQTT client disconnected successfully.")

        except Exception as e:
            self.log.exception(f"❌ MQTT publishing failed : {e}")
            raise e
