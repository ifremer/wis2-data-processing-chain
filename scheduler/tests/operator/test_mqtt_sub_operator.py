import json
import pytest
from unittest.mock import MagicMock, patch
from plugins.operator.mqtt_sub_operator import MqttSubOperator


@pytest.fixture
def operator():
    return MqttSubOperator(
        task_id="test_mqtt_listener",
        topic="test/topic",
        dag_ids_to_trigger=["dag1", "dag2"],
        mqtt_broker="localhost",
        mqtt_port=8081,
        mqtt_username="user",
        mqtt_password="pass",
        ssl_enabled=False,
    )


def test_operator_initialization(operator):
    assert operator.topic == "test/topic"
    assert operator.dag_ids_to_trigger == ["dag1", "dag2"]
    assert operator.port == 8081


@patch("paho.mqtt.client.Client")
def test_test_connection(mock_mqtt, operator):
    """Should connect and disconnect without error."""
    mock_client = MagicMock()
    mock_mqtt.return_value = mock_client

    operator.test_connection()

    mock_client.username_pw_set.assert_called_once_with("user", "pass")
    mock_client.connect.assert_called_once()
    mock_client.disconnect.assert_called_once()


# @patch("plugins.operator.mqtt_sub_operator.trigger_dag")
# def test_on_message_triggers_dag(mock_trigger_dag, operator):
#     """Simulate receiving a valid CloudEvent and triggering DAGs."""

#     # Simulate a valid CloudEvent JSON
#     message_body = {
#         "specversion": "1.0",
#         "type": "test.event",
#         "source": "test.source",
#         "id": "1234",
#         "time": "2024-01-01T00:00:00Z",
#         "data": {"hello": "world"},
#     }
#     payload = json.dumps(message_body)

#     mock_message = MagicMock()
#     mock_message.payload.decode.return_value = payload

#     # Call the private method directly
#     operator.execute = MagicMock()  # prevent full loop
#     on_message = operator.__class__.execute.__globals__["on_message"]
#     on_message(operator, None, mock_message)

#     assert mock_trigger_dag.call_count == len(operator.dag_ids_to_trigger)
#     for dag_id in operator.dag_ids_to_trigger:
#         mock_trigger_dag.assert_any_call(dag_id=dag_id, conf=pytest.ANY, replace_microseconds=False)
