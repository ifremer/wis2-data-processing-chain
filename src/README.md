# wis2_data_processing_chain

Python scripts dedicated to wWS2 data processing chain

## Get Started

- Validate STAC format

```bash
cd src
docker build -t wis2-data-processing-chain .
docker run -it --rm -e TASK_ID="validate-task" -e MESSAGE_STORE_DIR="/tmp" -e MESSAGE="$(cat ../data/event-message/bufr/bufr-creation-cloudevent.json)" -v /tmp:/tmp wis2-data-processing-chain python3 /app/wis2_data_processing_chain/standards/validate_stac.py
```

- Generate WIS2 notification message from STAC

```bash
cd src
docker build -t wis2-data-processing-chain .
docker run -it --rm -e TASK_ID="generate-task" -e MESSAGE_STORE_DIR="/tmp" -e MESSAGE="$(cat ../data/event-message/bufr/bufr-creation-cloudevent.json)" -v /tmp:/tmp wis2-data-processing-chain python3 /app/wis2_data_processing_chain/notifications/generate_notification_message.py
```
