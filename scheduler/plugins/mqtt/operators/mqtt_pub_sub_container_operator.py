"""
Airflow custom operator to publish/subscribe MQTT messages from within
a containerized environment (Apptainer / Singularity).

This operator wraps a container execution that runs a Python MQTT client
and injects connection parameters via environment variables.
"""

from __future__ import annotations

from airflow.providers.singularity.operators.singularity import SingularityOperator
from mqtt.hooks.mqtt_hook import MqttHook


class MqttPubSubContainerOperator(SingularityOperator):
    """
    Custom operator to run MQTT subscription logic inside a container.

    Inherits from SingularityOperator and wraps execution of `mqtt_client.py`.
    """

    def __init__(
        self,
        mqtt_conn_id: str,
        topic: str,
        message: dict = None,
        message_file: str = None,
        bind_directory: str = None,
        **kwargs,
    ):
        cfg = MqttHook(mqtt_conn_id).get_config()
        env_vars = {
            "MQTT_HOST": cfg.host,
            "MQTT_PORT": str(cfg.port),
            "MQTT_USERNAME": cfg.username,
            "MQTT_PASSWORD": cfg.password,
            "MQTT_TLS": str(cfg.tls),
            "MQTT_TLS_INSECURE": str(cfg.tls_insecure),
            "MQTT_TRANSPORT": cfg.transport,
            "MQTT_WS_PATH": cfg.ws_path,
            "MQTT_KEEPALIVE": str(cfg.keepalive),
            "MQTT_QOS": str(cfg.qos),
            "MQTT_TOPIC": topic,
            "APPTAINER_DOCKER_USERNAME": "{{ var.value.internal_registry_user }}",
            "APPTAINER_DOCKER_PASSWORD": "{{ var.value.internal_registry_password }}",
        }

        # On priorise l'envoi via fichier
        if message_file:
            env_vars["MQTT_MESSAGE_FILE"] = message_file
        elif message:
            env_vars["MQTT_MESSAGE"] = message

        # Bind directory if needed
        if bind_directory:
            env_vars["APPTAINER_BIND"] = bind_directory

        super().__init__(
            image="docker://gitlab-registry.ifremer.fr/ifremer-commons/docker/internal-images/mqtt-pub-sub:3.12-slim",
            # image="docker://mqtt-client:latest", TODO: try fully local images
            environment=env_vars,
            command=f"python /app/mqtt_client.py pub --topic {topic}",
            **kwargs,
        )
