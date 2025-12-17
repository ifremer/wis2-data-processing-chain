# wis2_data_processing_chain

Python scripts dedicated to wWS2 data processing chain

## Get Started

### Validate STAC format

docker build -t wis2-data-processing-chain .

docker run -it --rm -e TASK_ID="test-task" -e MESSAGE_STORE_DIR="/tmp" -e MESSAGE="$MESSAGE" -v /tmp:/tmp wis2-data-processing-chain python3 /app/wis2_data_processing_chain/standards/validate_stac.py