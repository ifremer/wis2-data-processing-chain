import time
from typing import Optional

import paho.mqtt.client as mqtt
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.models import Variable


class MqttPubOperator(BaseOperator):
    """
    Publish a message to a specified MQTT topic using WebSockets.
    """

    def __init__(
        self,
        topic: str,
        message: dict,
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
        self.message = message

        self.broker = mqtt_broker
        self.port = int(mqtt_port)
        self.username = mqtt_username
        self.password = mqtt_password
        self.ssl_enabled = ssl_enabled

    def execute(self):

        if not self.message:
            raise ValueError("❌ No message found or message is empty !")

        client = mqtt.Client(transport="websockets")
        if self.ssl_enabled:
            client.tls_set()

        client.username_pw_set(self.username, self.password)

        try:
            self.log.info(f"🔗 Connecting to MQTT broker at {self.broker}:{self.port}")
            client.connect(self.broker, self.port, 60)
            client.loop_start()

            result, mid = client.publish(self.topic, self.message)

            if result != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(
                    f"❌ Failed to publish message: result={result}, mid={mid}"
                )

            self.log.info(f"📤 Message published to {self.topic}:\n{self.message}")

            time.sleep(2)
            client.loop_stop()
            client.disconnect()
            self.log.info("🔌 Disconnected from MQTT broker.")
        except Exception as e:
            self.log.exception("❌ MQTT publishing failed")
            raise e
