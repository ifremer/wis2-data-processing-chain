import logging
import json
import os
import argparse
from wis2_data_processing_chain.utils.data_store import (
    save_message,
    recover_message,
)
from wis2_data_processing_chain.notifications.notification_message import (
    generate_notification_message_from_file,
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
        "--file",
        default=os.getenv("FILE"),
        help="WIS2 Core metadata file for Dataset",
    )

    return parser.parse_args()


def main():

    try:
        # get function arguments
        args = parse_args()

        if not args.file:
            raise ValueError("No file provided")

        notification = generate_notification_message_from_file(args.file)

        print(json.dumps(notification, indent=2))

    except Exception:
        logger.exception("❌ Generate WIS2 Metadata Notification failed.")
        raise


if __name__ == "__main__":
    main()
