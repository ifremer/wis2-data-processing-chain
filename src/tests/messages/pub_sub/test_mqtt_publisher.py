import json
from unittest.mock import patch, MagicMock
import wis2_data_processing_chain.messages.pub_sub.mqtt_publisher as mqtt_pub
import pytest

@pytest.fixture
def valid_json():
    return {"test": "data"}


@pytest.fixture
def valid_json_str(valid_json):
    return json.dumps(valid_json)


def test_load_message_from_string(valid_json_str):
    result = mqtt_pub.load_message(valid_json_str, None)
    assert result == valid_json_str


def test_load_message_plain_text():
    msg = "just a string"
    result = mqtt_pub.load_message(msg, None)
    assert result == msg


def test_load_message_raises_with_nothing():
    with pytest.raises(ValueError):
        mqtt_pub.load_message(None, None)


@patch("mqtt_pub.mqtt.Client")
def test_main_success(mock_mqtt_client, tmp_path, valid_json_str):
    file_path = tmp_path / "message.json"
    file_path.write_text(valid_json_str)

    mock_client_instance = MagicMock()
    mock_client_instance.publish.return_value = (0, 123)
    mock_mqtt_client.return_value = mock_client_instance

    test_args = [
        "--topic", "test/topic",
        "--message_file", str(file_path),
        "--broker", "localhost",
        "--port", "1883",
        "--username", "user",
        "--password", "pass"
    ]

    with patch("sys.argv", ["mqtt_pub.py"] + test_args):
        mqtt_pub.main()

    mock_client_instance.connect.assert_called_once()
    mock_client_instance.publish.assert_called_once_with("test/topic", valid_json_str)
    mock_client_instance.disconnect.assert_called_once()


@patch("mqtt_pub.mqtt.Client")
def test_main_publish_fails(mock_mqtt_client, tmp_path, valid_json_str):
    file_path = tmp_path / "message.json"
    file_path.write_text(valid_json_str)

    mock_client_instance = MagicMock()
    mock_client_instance.publish.return_value = (1, 999)  # Simulate error
    mock_mqtt_client.return_value = mock_client_instance

    test_args = [
        "--topic", "test/topic",
        "--message_file", str(file_path),
        "--broker", "localhost"
    ]

    with patch("sys.argv", ["mqtt_pub.py"] + test_args), pytest.raises(SystemExit):
        mqtt_pub.main()
