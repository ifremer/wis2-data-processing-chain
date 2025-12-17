import os
import logging
import argparse
from wis2_data_processing_chain.utils.data_store import (
    cleanup_message_storage,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="")

    parser.add_argument(
        "--run_id",
        default=os.getenv("RUN_ID"),
        help="Current Airflow run id",
    )

    return parser.parse_args()


def main():
    # get function arguments
    args = parse_args()

    deleted = cleanup_message_storage(args.run_id)
    if deleted:
        logger.info("🧹 Message backup cleaned up successfully.")
    else:
        logger.warning("⚠️ No message backup found to clean.")


if __name__ == "__main__":
    main()
