from plugins.operator.mqtt_sub_operator import MqttSubOperator
from plugins.operator.mqtt_pub_operator import MqttPubOperator
from airflow.plugins_manager import AirflowPlugin


class MqttPlugin(AirflowPlugin):
    name = "mqtt_plugin"
    operators = [MqttSubOperator, MqttPubOperator]
