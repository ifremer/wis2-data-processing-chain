import json
import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)
BASE_DIR = Path("/tmp/messages/dags")


def save_message(dag_id: str, run_id: str, task_id: str, message: dict) -> Path:
    """
    Save the raw input message for a DAG run.
    Returns the path where the message is stored.
    """
    try:
        dir_path = BASE_DIR / dag_id / run_id
        dir_path.mkdir(parents=True, exist_ok=True)

        message_path = dir_path / f"{task_id}.json"

        with open(message_path, "w", encoding="utf-8") as f:
            json.dump(message, f, indent=4)

        logger.info(f"💾 Store message on file system : {message_path}")
    except Exception as e:
        logger.error(f"❌ Store message Failed : {e}", exc_info=True)
        raise

    return message_path


def load_message(dag_id: str, run_id: str, task_id: str) -> dict:
    """Load a saved message from a file."""
    dir_path = BASE_DIR / dag_id / run_id
    message_path = dir_path / f"{task_id}.json"
    print(message_path)
    with open(message_path, "r", encoding="utf-8") as f:
        return json.load(f)


def cleanup_message_storage(dag_id: str, run_id: str) -> bool:
    """
    Delete stored messages for a specific DAG run.
    Returns True if cleanup was successful.
    """
    dir_path = BASE_DIR / dag_id / run_id
    if dir_path.exists() and dir_path.is_dir():
        shutil.rmtree(dir_path)
        return True
    return False
