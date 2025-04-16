from airflow.plugins_manager import AirflowPlugin
from scheduler.plugins.operator.mqtt_operator import MqttSubOperator

class MqttPlugin(AirflowPlugin):
    name = "mqtt_plugin"
    operators = [MqttSubOperator]
