"""
Airflow MQTT hook.

This module defines:
- a dataclass holding normalized MQTT connection configuration
- a hook that reads an Airflow Connection and converts it into this config
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from airflow.hooks.base import BaseHook
from mqtt.utils import as_bool, norm_transport


@dataclass
class MqttConnConfig:  # pylint: disable=too-many-instance-attributes
    """
    Normalized MQTT connection configuration.

    This object is built from an Airflow Connection and is meant to be reused
    by operators, sensors and triggers without re-parsing conn.extra.
    """

    host: str
    port: int = 1883
    username: Optional[str] = None
    password: Optional[str] = None
    tls: bool = False
    tls_insecure: bool = False
    ca_certs: Optional[str] = None
    certfile: Optional[str] = None
    keyfile: Optional[str] = None
    transport: str = "ws"  # "tcp" | "ws"
    ws_path: str = "/"
    keepalive: int = 60
    qos: int = 1
    proxy: Optional[str] = None  # ex: http://user:pwd@proxy:3128 (WS only)


class MqttHook(BaseHook):
    """
    Airflow Hook for MQTT connections.

    Reads an Airflow Connection (conn_id) and produces a :class:`MqttConnConfig`.

    - Connection host/port/login/password → base parameters
    - conn.extra (JSON) → optional fields (TLS, transport, proxy, QoS, etc.)
    """

    conn_name_attr = "mqtt_conn_id"
    default_conn_name = "mqtt_default"
    conn_type = "mqtt"
    hook_name = "MQTT"

    def __init__(self, mqtt_conn_id: str = "mqtt_default") -> None:
        super().__init__()
        self.mqtt_conn_id = mqtt_conn_id

    def get_config(self) -> MqttConnConfig:
        """
        Build and return a normalized MQTT configuration from the Airflow Connection.
        """
        conn = self.get_connection(self.mqtt_conn_id)
        extra: Dict[str, Any] = conn.extra_dejson or {}

        return MqttConnConfig(
            host=conn.host,
            port=int(conn.port or (8081 if extra.get("transport") == "ws" else 1883)),
            username=conn.login or None,
            password=conn.password or None,
            tls=as_bool(extra.get("tls")),
            tls_insecure=as_bool(extra.get("tls_insecure", False)),
            ca_certs=extra.get("ca_certs"),
            certfile=extra.get("certfile"),
            keyfile=extra.get("keyfile"),
            transport=norm_transport(extra.get("transport")),
            ws_path=extra.get("ws_path", "/"),
            keepalive=int(extra.get("keepalive", 60)),
            qos=int(extra.get("qos", 1)),
            proxy=extra.get("proxy"),  # WS only
        )
