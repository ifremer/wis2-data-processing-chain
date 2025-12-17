import logging
import os
import argparse
import json
from wis2_data_processing_chain.utils.data_store import (
    save_message,
    recover_message,
)
from wis2_data_processing_chain.standards.stac_standard import (
    validate_stac_item,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="")

    parser.add_argument(
        "--task_id",
        default=os.getenv("TASK_ID"),
        help="Current Airflow task id",
    )
    parser.add_argument(
        "--message",
        default=os.getenv("MESSAGE"),
        help="CloudEvents message as string or JSON string",
    )

    return parser.parse_args()


def main():
    cloudevents_message = None

    try:
        # get function arguments
        args = parse_args()

        # Recover execution → get message store if exists
        cloudevents_message = recover_message(args.task_id)

        # Normal execution → get message from xcom
        if cloudevents_message is None:
            cloudevents_message = args.message
            # Store message in file system in case of error during process
            save_message(args.task_id, args.message)
            logger.info("💾 Saved incoming message to disk, in case of error.")

        logger.info(f"cloudevents_message = {cloudevents_message}")
        cloudevents_message = json.loads(cloudevents_message)
        stac_item_json = cloudevents_message.get("data")
        validate_stac_item(stac_item_json)
    except Exception:
        logger.exception("❌ STAC validation Failed")
        raise


if __name__ == "__main__":
    main()
