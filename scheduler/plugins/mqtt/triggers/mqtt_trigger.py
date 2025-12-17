from __future__ import annotations
import asyncio, base64, ssl, re
import logging
from typing import Any, AsyncIterator, Dict, Optional
from mqtt.utils import as_bool, norm_transport
from airflow.triggers.base import BaseTrigger, TriggerEvent


class MqttMessageTrigger(BaseTrigger):
    """
    Attend le premier message sur `topic` (regex optionnelle sur payload),
    via paho-mqtt (TCP ou WebSockets), avec support MQTT v5 (session persistante).
    """

    def __init__(
        self,
        host: str,
        topic: str,
        port: int = 1883,
        username: Optional[str] = None,
        password: Optional[str] = None,
        tls: bool = False,
        tls_insecure: bool = False,
        ca_certs: Optional[str] = None,
        certfile: Optional[str] = None,
        keyfile: Optional[str] = None,
        transport: str = "ws",  # "tcp" | "websockets" | "ws"
        ws_path: str = "/",
        client_id: Optional[str] = None,
        keepalive: int = 60,
        qos: int = 1,  # QoS>=1 recommandé pour at-least-once
        message_regex: Optional[str] = None,
        timeout: Optional[float] = None,
        proxy: Optional[str] = None,  # WS uniquement
        # --- Nouveaux paramètres v5 / persistance ---
        protocol: str = "v5",  # "v5" | "v311"
        session_expiry: int = 3600,  # secondes; >0 => session persistante
        no_local: bool = False,
        retain_as_published: bool = False,
        retain_handling: int = 0,  # 0=send retained; 1=only-new; 2=never
        shared_group: Optional[str] = None,  # $share/<group>/<topic>
        # v3.1.1:
        clean_session: Optional[bool] = False,
    ) -> None:
        super().__init__()
        self.host = host
        self.topic = topic
        self.port = int(port)
        self.username = username
        self.password = password
        self.tls = as_bool(tls)
        self.tls_insecure = as_bool(tls_insecure)
        self.ca_certs = ca_certs
        self.certfile = certfile
        self.keyfile = keyfile
        self.transport = norm_transport(transport)  # "websockets" ou "tcp"
        self.ws_path = ws_path
        self.client_id = client_id
        self.keepalive = int(keepalive)
        self.qos = int(qos)
        self.message_regex = message_regex
        self._regex = re.compile(message_regex) if message_regex else None
        self.timeout = timeout
        self.proxy = proxy

        self.protocol = protocol
        self.session_expiry = int(session_expiry)
        self.no_local = as_bool(no_local)
        self.retain_as_published = as_bool(retain_as_published)
        self.retain_handling = int(retain_handling)
        self.shared_group = shared_group
        self.clean_session = as_bool(clean_session) if protocol != "v5" else None

    def _safe_proxy_desc(self) -> str:
        if not self.proxy:
            return "-"
        try:
            from urllib.parse import urlparse

            p = urlparse(self.proxy)
            host = p.hostname or "?"
            port = p.port or "-"
            return f"{p.scheme or 'http'}://{host}:{port}"
        except Exception:
            return "<invalid-proxy>"

    def serialize(self) -> tuple[str, Dict[str, Any]]:
        return (
            "mqtt.triggers.mqtt_trigger.MqttMessageTrigger",
            {
                "host": self.host,
                "topic": self.topic,
                "port": self.port,
                "username": self.username,
                "password": self.password,
                "tls": self.tls,
                "tls_insecure": self.tls_insecure,
                "ca_certs": self.ca_certs,
                "certfile": self.certfile,
                "keyfile": self.keyfile,
                "transport": norm_transport(self.transport),
                "ws_path": self.ws_path,
                "client_id": self.client_id,
                "keepalive": self.keepalive,
                "qos": self.qos,
                "message_regex": self.message_regex,
                "timeout": self.timeout,
                "proxy": self.proxy,
                "protocol": self.protocol,
                "session_expiry": self.session_expiry,
                "no_local": self.no_local,
                "retain_as_published": self.retain_as_published,
                "retain_handling": self.retain_handling,
                "shared_group": self.shared_group,
                "clean_session": self.clean_session,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            import paho.mqtt.client as mqtt
            from urllib.parse import urlparse

            try:
                from paho.mqtt.subscribeoptions import SubscribeOptions
            except Exception:
                SubscribeOptions = None
            try:
                from paho.mqtt.properties import Properties
                from paho.mqtt.packettypes import PacketTypes
            except Exception:
                Properties = None
                PacketTypes = None
        except Exception as e:
            self.log.exception("paho import error")
            yield TriggerEvent(
                {"status": "error", "message": f"paho import error: {e}"}
            )
            return

        loop = asyncio.get_running_loop()
        q: asyncio.Queue = asyncio.Queue()

        # --------- Callbacks ----------
        def on_connect(client, userdata, flags, rc, properties=None):
            session_present = getattr(flags, "session_present", False)
            if session_present:
                self.log.info("connected (session present) -> (re)subscribe for safety")

            sub_topic = (
                f"$share/{self.shared_group}/{self.topic}"
                if self.shared_group
                else self.topic
            )

            self.log.info("subscribe '%s' qos=%s", sub_topic, max(1, self.qos))
            client.subscribe(sub_topic, qos=max(1, self.qos))

        def on_subscribe(client, userdata, mid, granted_qos, properties=None):
            self.log.info("subscribed mid=%s granted_qos=%s", mid, granted_qos)

        def on_message(client, userdata, msg):
            raw = msg.payload or b""

            try:
                payload = raw.decode("utf-8")
                encoding = "text"
            except UnicodeDecodeError:
                payload = base64.b64encode(raw).decode("ascii")
                encoding = "base64"

            # Filtre (tel que tu le fais aujourd'hui : sur payload texte OU base64)
            if self._regex and not self._regex.search(payload):
                return

            event = {
                "status": "message",
                "topic": msg.topic,
                "qos": msg.qos,
                "payload": payload,
                "payload_encoding": encoding,
                "message_size": len(raw),
            }

            loop.call_soon_threadsafe(q.put_nowait, event)

        def on_disconnect(client, userdata, disconnect_flags, rc, properties=None):
            # rc/reason_code: 0 = normal disconnect
            self.log.info("disconnected rc=%s flags=%s", rc, disconnect_flags)

            if rc not in (0, None):
                loop.call_soon_threadsafe(
                    q.put_nowait,
                    {"status": "error", "message": f"disconnect rc={rc}"},
                )

        # --------- Client ----------
        client_kwargs: Dict[str, Any] = {
            "client_id": self.client_id,
            "transport": (
                "websockets" if self.transport in ("ws", "websockets") else "tcp"
            ),
            "callback_api_version": getattr(mqtt.CallbackAPIVersion, "VERSION2", 2),
            "protocol": mqtt.MQTTv5 if self.protocol == "v5" else mqtt.MQTTv311,
        }

        # activate session (must be combine with QoS)
        if self.protocol != "v5" and self.clean_session is not None:
            client_kwargs["clean_session"] = bool(self.clean_session)

        client = mqtt.Client(**client_kwargs)
        # Ordre strict si souhaité (sinon commente)
        try:
            client.max_inflight_messages_set(1)
        except Exception:
            pass

        # TLS
        if self.tls:
            ctx = ssl.create_default_context()
            if self.ca_certs:
                ctx.load_verify_locations(cafile=self.ca_certs)
            if self.certfile and self.keyfile:
                ctx.load_cert_chain(certfile=self.certfile, keyfile=self.keyfile)
            ctx.check_hostname = not self.tls_insecure
            client.tls_set_context(ctx)
            client.tls_insecure_set(self.tls_insecure)

        # Auth
        client.username_pw_set(self.username or None, self.password or None)

        # WebSockets
        if client_kwargs["transport"] == "websockets":
            client.ws_set_options(path=self.ws_path or "/")
            if self.proxy:
                try:
                    p = urlparse(self.proxy)
                    client.proxy_set(
                        proxy_type=p.scheme or "http",
                        proxy_addr=p.hostname,
                        proxy_port=p.port or 3128,
                        proxy_username=p.username,
                        proxy_password=p.password,
                    )
                except Exception:
                    pass

        client.on_connect = on_connect
        client.on_subscribe = on_subscribe
        client.on_message = on_message
        client.on_disconnect = on_disconnect

        # --------- Connect & loop ---------
        try:
            if self.protocol == "v5" and Properties and PacketTypes:
                props = mqtt.Properties(mqtt.PacketTypes.CONNECT)
                # > 0 for persistence
                props.SessionExpiryInterval = self.session_expiry
                client.connect(
                    self.host,
                    self.port,
                    keepalive=self.keepalive,
                    clean_start=self.clean_session,  # must be combine with QoS
                    properties=props,
                )
            else:
                client.connect(self.host, self.port, keepalive=self.keepalive)

            client.loop_start()

            # ---- Wait for the first event, then drain a short window ----
            try:
                first = await asyncio.wait_for(q.get(), timeout=self.timeout)
            except asyncio.TimeoutError:
                yield TriggerEvent({"status": "timeout"})
                return

            batch = [first]
            idle_window_s = 0.8  # tune 0.5–2.0s depending on bursts
            cap = 1000  # safety bound

            while len(batch) < cap:
                try:
                    batch.append(await asyncio.wait_for(q.get(), timeout=idle_window_s))
                except asyncio.TimeoutError:
                    break

            self.log.info("batch received: %s message(s)", len(batch))
            yield TriggerEvent({"status": "batch", "count": len(batch), "messages": batch})
            return  # ✅ stop here; the finally block will close the client

        except Exception as e:
            yield TriggerEvent({"status": "error", "message": repr(e)})

        finally:
            try:
                client.disconnect()
                # laisse une micro-fenêtre au loop thread pour envoyer DISCONNECT
                await asyncio.sleep(0.1)
                client.loop_stop(force=False)
            except Exception:
                pass
            self.log.info("client closed")
