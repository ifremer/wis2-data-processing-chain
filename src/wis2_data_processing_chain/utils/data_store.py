import json
import logging
import os
import shutil
from pathlib import Path
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

BASE_DIR = Path(os.getenv("MESSAGE_STORE_DIR", "/data")).resolve()


def save_message(task_id: str, message: dict) -> Path:
    """
    Save the raw input message for a DAG run.
    Returns the path where the message is stored.
    """
    try:
        BASE_DIR.mkdir(parents=True, exist_ok=True)
        message_path = BASE_DIR / f"{task_id}.json"

        with open(message_path, "w", encoding="utf-8") as f:
            json.dump(message, f, indent=4)

        logger.info(f"💾 Store message on file system : {message_path}")
    except Exception:
        logger.exception("❌ Store message Failed")
        raise

    return message_path


def load_message(task_id: str) -> dict:
    """Load a saved message from a file."""
    message_path = BASE_DIR / f"{task_id}.json"
    with open(message_path, "r", encoding="utf-8") as f:
        return json.load(f)


def cleanup_message_storage(run_id: str) -> bool:
    """
    Delete stored messages for a specific DAG run.
    Returns True if cleanup was successful.
    """
    dag_run_path = BASE_DIR / run_id
    logger.info(f"🧼 Cleanning {dag_run_path}...")
    if dag_run_path.exists() and dag_run_path.is_dir():
        shutil.rmtree(dag_run_path)
        return True
    return False


def recover_message(task_id: str) -> Optional[Dict[str, Any]]:
    """
    Recharge le message depuis le disque pour le DAG run courant, si présent.
    """
    try:
        msg = load_message(task_id)
        logger.info("♻️ Reloaded message from disk.")
        return msg
    except FileNotFoundError:
        logger.info("🥇 No data to reload, loading incoming message")
        return None
    except Exception:
        logger.exception("❌ Failed to reload message")
        return None
