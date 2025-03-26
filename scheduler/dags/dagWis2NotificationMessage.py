import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def process_files(**kwargs):
    """Récupère le message MQTT et traite le fichier."""
    message = kwargs["dag_run"].conf

    if not message or not isinstance(message, dict) or "filename" not in message:
        logging.error("❌ Message MQTT invalide ou absent")
        return

    logging.info(f"✅ Traitement du fichier : {message['filename']}")


# Définition du DAG qui traite les messages
process_message_dag = DAG(
    dag_id="process_message_dag",
    dag_display_name="📂 Process Message DAG",
    default_args={
        "owner": "lbruvryl",
        "start_date": datetime(2025, 3, 24),
        "retries": 3,
    },
    description="DAG déclenché pour traiter un message MQTT.",
    schedule_interval=None,
    catchup=False,
)

# Définition du DAG déclenché par le DAG wis2-listener-production-file
process_message_dag = DAG(
    dag_id="wis2-publish-message-notification",
    dag_display_name="🔔 WIS2 - Publication de messages de notifications",
    default_args={
        "owner": "lbruvryl",
        "email": ["lbruvryl@ifremer.fr"],
        "email_on_failure": False,
        "email_on_retry": False,
        "start_date": datetime(2025, 3, 24),
        "retries": 3,
    },
    description="Envoi de messages de notifications MQTT pour WMO Information System (WIS2).",
    schedule_interval=None,  # Permet au DAG de tourner en continu
    catchup=False,
)

# Opérateur pour traiter le message
process_files_task = PythonOperator(
    task_id="process_files",
    python_callable=process_files,
    provide_context=True,
    dag=process_message_dag,
)
