from __future__ import annotations
from typing import Any, Optional, Dict, List
from datetime import timedelta
import json
from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import TriggerEvent
from airflow.providers.http.hooks.http import HttpHook
from mqtt.hooks.mqtt_hook import MqttHook
from mqtt.triggers.mqtt_trigger import MqttMessageTrigger


class MqttMessageSensor(BaseSensorOperator):
    """
    Sensor déférable "stream infini" :
      - Attend un (petit) batch de messages MQTT (drain window côté Trigger).
      - Déclenche 1 DAG aval par message (si fanout_dag_id est fourni).
      - Se re-défère immédiatement pour écouter la suite (connexion quasi-permanente).

    Champs templatisés : topic, message_regex, shared_group, client_id, fanout_dag_id, airflow_api_conn_id.
    """

    template_fields = (
        "topic",
        "message_regex",
        "shared_group",
        "client_id",
        "fanout_dag_id",
        "airflow_api_conn_id",
    )

    def __init__(
        self,
        mqtt_conn_id: str,
        topic: str,
        message_regex: Optional[str] = None,
        timeout: Optional[float] = None,
        client_id: Optional[str] = None,
        # ----- MQTT v5 / subscription options -----
        protocol: Optional[str] = None,  # "v5" (défaut) | "v311"
        shared_group: Optional[str] = None,  # $share/<group>/topic
        session_expiry: Optional[int] = None,  # secondes; >0 => session persistante
        clean_session: Optional[bool] = None,  # v3.1.1 uniquement
        no_local: Optional[bool] = None,
        retain_as_published: Optional[bool] = None,
        retain_handling: Optional[int] = None,  # 0 send retained | 1 only-new | 2 never
        qos: int = 1,  # override QoS de la Connection
        # ----- Trigger Dag -----
        fanout_dag_id: Optional[
            str
        ] = None,  # si défini, déclenche ce DAG pour chaque message
        fanout_conf_extra: Optional[Dict[str, Any]] = None,  # conf additionnelle
        airflow_api_conn_id: str = "airflow_api",
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.mqtt_conn_id = mqtt_conn_id
        self.topic = topic
        self.message_regex = message_regex
        self.timeout = timeout
        self.client_id = client_id

        # overrides
        self.protocol = protocol or "v5"
        self.shared_group = shared_group
        # ↑ par défaut 30 jours pour matcher Mosquitto 'persistent_client_expiration 30d'
        self.session_expiry = (
            session_expiry if session_expiry is not None else 30 * 24 * 3600
        )
        self.clean_session = clean_session
        self.no_local = False if no_local is None else bool(no_local)
        self.retain_as_published = (
            False if retain_as_published is None else bool(retain_as_published)
        )
        self.retain_handling = 0 if retain_handling is None else int(retain_handling)
        self.qos_override = qos

        # fan-out
        self.fanout_dag_id = fanout_dag_id
        self.fanout_conf_extra = fanout_conf_extra or {}
        self.airflow_api_conn_id = airflow_api_conn_id

    # ---------- phase 1 : on se déferre pour écouter ----------
    def execute(self, context):
        cfg = MqttHook(self.mqtt_conn_id).get_config()
        # on prend le qos du sensor si non défini on regarde si il l'est au niveau du hook
        qos_final = self.qos_override if self.qos_override is not None else cfg.qos

        trigger = MqttMessageTrigger(
            host=cfg.host,
            port=cfg.port,
            username=cfg.username,
            password=cfg.password,
            tls=cfg.tls,
            tls_insecure=cfg.tls_insecure,
            ca_certs=cfg.ca_certs,
            certfile=cfg.certfile,
            keyfile=cfg.keyfile,
            transport=cfg.transport,
            ws_path=cfg.ws_path,
            client_id=self.client_id,
            keepalive=cfg.keepalive,
            qos=qos_final,
            message_regex=self.message_regex,
            timeout=self.timeout,
            proxy=cfg.proxy,
            topic=self.topic,
            # v5 & co
            protocol=self.protocol,
            session_expiry=self.session_expiry,
            no_local=self.no_local,
            retain_as_published=self.retain_as_published,
            retain_handling=self.retain_handling,
            shared_group=self.shared_group,
            clean_session=self.clean_session,
        )

        self.log.info(
            "MQTT listen start (stream): host=%s port=%s topic=%s client_id=%s qos=%s proto=%s clean_session=%s session_expiry=%s",
            cfg.host,
            cfg.port,
            self.topic,
            self.client_id,
            qos_final,
            self.protocol,
            self.clean_session,
            self.session_expiry,
        )
        self.defer(
            trigger=trigger,
            method_name="execute_complete",
            timeout=timedelta(minutes=30),
        )

    # ---------- phase 2 : on traite le batch puis on se re-défère ----------
    def execute_complete(self, context, event: Optional[TriggerEvent] = None):

        if not event or not isinstance(event, dict):
            # on repart écouter (stream infini) même sans event valide
            self.log.warning("MQTT: event manquant/incorrect, on repart en écoute.")
            return self._rearm(context)

        status = event.get("status")

        if status == "timeout":
            # écoute continue : on repart attendre
            self.log.info("MQTT: timeout, on repart en écoute.")
            return self._rearm(context)

        if status not in ("message", "batch"):
            # erreur: on log puis repart
            self.log.warning("MQTT: event inattendu: %s", event)
            return self._rearm(context)

        # Normalise en liste
        msgs: List[Dict[str, Any]] = event["messages"] if status == "batch" else [event]
        self.log.info("MQTT: batch reçu: %d message(s)", len(msgs))

        # Fan-out via REST API Airflow v1 DAG Runs (compatible Airflow 3)
        if self.fanout_dag_id:
            hook = HttpHook(method="POST", http_conn_id=self.airflow_api_conn_id)
            token = self._get_airflow_api_token(hook)
            created = 0
            headers = {"Authorization": f"Bearer {token}"}
            for m in msgs:
                conf = {"message_event": m, "message": m.get("payload", "")}
                if self.fanout_conf_extra:
                    conf.update(self.fanout_conf_extra)
                body = {
                    "logical_date": None,
                    "conf": conf,
                }
                endpoint = f"api/v2/dags/{self.fanout_dag_id}/dagRuns"
                resp = hook.run(endpoint=endpoint, json=body, headers=headers)
                if not (200 <= resp.status_code < 300):
                    # On log et on continue ; pas d’ORM, pas de session locale
                    self.log.error(
                        "Fanout POST %s failed: %s %s",
                        endpoint,
                        resp.status_code,
                        resp.text,
                    )
                else:
                    created += 1
            self.log.info("MQTT: %d run(s) créés sur '%s'", created, self.fanout_dag_id)
        else:
            # Mode legacy : on expose le dernier message en XCom si quelqu’un en dépend
            if msgs:
                # XCom via SDK est autorisé en 3.x (utilisé par BaseOperator)
                self.xcom_push(context, key="last_message_event", value=msgs[-1])
            self.log.info("MQTT: batch sans fanout (dernier message poussé en XCom).")

        # Stream infini : on repart immédiatement écouter
        return self._rearm(context)

    # ---------- helper : re-déférer ----------
    def _rearm(self, context):
        cfg = MqttHook(self.mqtt_conn_id).get_config()
        qos_final = self.qos_override if self.qos_override is not None else cfg.qos

        trigger = MqttMessageTrigger(
            host=cfg.host,
            port=cfg.port,
            username=cfg.username,
            password=cfg.password,
            tls=cfg.tls,
            tls_insecure=cfg.tls_insecure,
            ca_certs=cfg.ca_certs,
            certfile=cfg.certfile,
            keyfile=cfg.keyfile,
            transport=cfg.transport,
            ws_path=cfg.ws_path,
            client_id=self.client_id,
            keepalive=cfg.keepalive,
            qos=qos_final,
            message_regex=self.message_regex,
            timeout=self.timeout,
            proxy=cfg.proxy,
            topic=self.topic,
            protocol=self.protocol,
            session_expiry=self.session_expiry,
            no_local=self.no_local,
            retain_as_published=self.retain_as_published,
            retain_handling=self.retain_handling,
            shared_group=self.shared_group,
            clean_session=self.clean_session,
        )

        # IMPORTANT : on ne "return" rien → la tâche ne se termine pas
        self.defer(
            trigger=trigger,
            method_name="execute_complete",
            timeout=timedelta(minutes=30),
        )

    # ---------- helper : get API token ----------
    def _get_airflow_api_token(self, hook: HttpHook) -> str:
        conn = hook.get_connection(self.airflow_api_conn_id)
        extra = conn.extra_dejson or {}
        username = conn.login or extra.get("username")
        password = conn.password or extra.get("password")

        if not username or not password:
            raise AirflowException(
                "Airflow API connection must define a username/password to request a token"
            )

        login_payload = {"username": username, "password": password}
        resp = hook.run(endpoint="/auth/token", json=login_payload)
        if not (200 <= resp.status_code < 300):
            raise AirflowException(
                f"Unable to authenticate against Airflow API ({resp.status_code}): {resp.text}"
            )

        data = resp.json()
        token = data.get("access_token")
        if not token:
            raise AirflowException("Airflow API login did not return an access_token")

        return token
