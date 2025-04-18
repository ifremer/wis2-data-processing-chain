import pytest
from unittest.mock import patch, MagicMock
from plugins.operator.mqtt_pub_operator import MqttPubOperator


def test_operator_initialization():
    op = MqttPubOperator(
        task_id="test_pub",
        topic="test/topic",
        message={"key": "value"},
        mqtt_broker="localhost",
        mqtt_port=1883,
        mqtt_username="test",
        mqtt_password="test",
        ssl_enabled=False,
    )

    assert op.topic == "test/topic"
    assert isinstance(op.message, dict)
    assert op.broker == "localhost"
    assert op.port == 1883


@patch("plugins.operator.mqtt_pub_operator.mqtt.Client")
def test_execute_success(mock_mqtt_client_class):
    mock_client = MagicMock()
    mock_mqtt_client_class.return_value = mock_client

    mock_client.publish.return_value = (0, 1)

    op = MqttPubOperator(
        task_id="test_pub",
        topic="test/topic",
        message={"msg": "hello"},
        mqtt_broker="localhost",
        mqtt_port=8081,
        mqtt_username="user",
        mqtt_password="pass",
        ssl_enabled=False,
    )

    op.execute(context={})  # Fake Airflow context

    mock_client.connect.assert_called_once_with("localhost", 8081, 60)
    mock_client.publish.assert_called_once()
    args, kwargs = mock_client.publish.call_args
    assert args[0] == "test/topic"
    assert isinstance(args[1], str)
    assert "hello" in args[1]


@patch("plugins.operator.mqtt_pub_operator.mqtt.Client")
def test_execute_failure_on_publish(mock_mqtt_client_class):
    mock_client = MagicMock()
    mock_mqtt_client_class.return_value = mock_client
    mock_client.publish.return_value = (1, 1)  # simulate error

    op = MqttPubOperator(
        task_id="test_pub",
        topic="test/topic",
        message={"msg": "fail"},
        mqtt_broker="localhost",
        mqtt_port=8081,
        mqtt_username="user",
        mqtt_password="pass",
        ssl_enabled=False,
    )

    with pytest.raises(RuntimeError):
        op.execute(context={})
