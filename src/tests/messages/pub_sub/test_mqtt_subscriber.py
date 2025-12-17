# tests/test_mqtt_sub.py
import json
from unittest.mock import MagicMock
import wis2_data_processing_chain.messages.pub_sub.mqtt_subscriber as mqtt_sub


def test_on_message_valid_event(monkeypatch):
    mock_client = MagicMock()
    body = {"test": "data"}
    mock_payload = json.dumps(
        {
            "specversion": "1.0",
            "type": "com.example.event",
            "source": "test/source",
            "id": "abc-123",
            "data": body,
        }
    )

    mock_msg = MagicMock()
    mock_msg.payload.decode.return_value = mock_payload

    monkeypatch.setattr("mqtt_sub.DAG_IDS", ["test_dag"])
    mock_trigger = MagicMock()
    monkeypatch.setattr("mqtt_sub.Client", lambda: MagicMock(trigger_dag=mock_trigger))

    mqtt_sub.on_message(mock_client, None, mock_msg)
    mock_trigger().trigger_dag.assert_called_once_with(
        dag_id="test_dag", conf=json.dumps(body)
    )


def test_main_fails_if_missing_env(monkeypatch):
    monkeypatch.setenv("MQTT_TOPIC", "")
    monkeypatch.setenv("DAG_IDS", "")
    try:
        mqtt_sub.main()
    except SystemExit as e:
        assert e.code == 1
