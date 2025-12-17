from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, Any
from mqtt.utils import as_bool, norm_transport
from airflow.hooks.base import BaseHook


@dataclass
class MqttConnConfig:
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
    Lit une Connection Airflow (conn_id) et produit une MqttConnConfig.
    - Conn host/port/login/password → basiques
    - conn.extra (JSON) → champs optionnels (tls, ws_path, proxy, etc.)
    """

    conn_name_attr = "mqtt_conn_id"
    default_conn_name = "mqtt_default"
    conn_type = "mqtt"
    hook_name = "MQTT"

    def __init__(self, mqtt_conn_id: str = "mqtt_default") -> None:
        super().__init__()
        self.mqtt_conn_id = mqtt_conn_id

    def get_config(self) -> MqttConnConfig:
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
            qos=int(extra.get("qos", 0)),
            proxy=extra.get("proxy"),  # pour WS
        )
