import logging
import json
import os
import argparse
from wis2_data_processing_chain.utils.data_store import (
    save_message,
    recover_message,
)
from wis2_data_processing_chain.notifications.notification_message import (
    generate_notification_message_from_stac,
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
        help="STAC message as string or JSON string",
    )

    return parser.parse_args()


def main():

    try:
        # get function arguments
        args = parse_args()

        # Recover execution → get message store if exists
        cloudevents_message = recover_message(args.task_id)

        # Normal execution → get message from xcom
        if cloudevents_message is None:
            cloudevents_message = json.loads(args.message)

        # Get STAC item from message
        stac_item_message = cloudevents_message.get("data")

        notification_message = generate_notification_message_from_stac(
            stac_item_message
        )
        if notification_message is not None:
            # notification_message message in file system in case of error during process
            save_message(args.task_id, notification_message)
            logger.info("💾 Saved incoming message to disk, in case of error.")

    except Exception:
        logger.exception("❌ Generate WIS2 Notification failed.")
        raise


if __name__ == "__main__":
    main()
