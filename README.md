# Summary

This project demonstrates a processing chain for creating, validating, and publishing a message on the [WMO Information System](https://community.wmo.int/en/activity-areas/wis/wis2-implementation) (WIS 2.0).

The process is triggered when a data file is stored into a file system (this part is out of scope for this project). This event is transmitted via an MQTT broker (Mosquitto) and captured by a scheduler (Airflow), which schedule the processing chain to generate, validate and send a notification to the WMO Information System.

For demonstration purposes, the Argo use case is implemented. However, this workflow is designed to be adaptable to any data source, provided that an event is emitted when a new data file is created.

## Tools / Technologies

List of tools and technologies used:

- `Mosquitto`: MQTT message broker used to transmit events and notifications.
- `Airflow`: Scheduler that subscribes to the MQTT broker and orchestrates processing based on received events.
- `Python`: Processing tasks dedicated to WIS2 are written in Python and use the following libraries:
  - `Paho`: Python MQTT client for publishing and subscribing to topics.
  - `pywis_pubsub`: Validates data notification messages.
  - `pywcmp`: Validates notification messages of type WMO WIS Core Metadata Profile (WCMP).

## Architecture

```{mermaid}
flowchart TB;
%% Styles améliorés
classDef filesystem fill:#FFD700,stroke:#B8860B,stroke-width:2px,color:#000,font-weight:bold;
classDef process fill:#0A89B0,stroke:#08637F,stroke-width:2px,color:#fff,font-weight:bold;
classDef broker fill:#007acc,stroke:#005f99,stroke-width:2px,color:#fff,font-weight:bold;
classDef listener fill:#34D399,stroke:#0F9D58,stroke-width:2px,color:#fff,font-weight:bold;
classDef airflow fill:#888f8a,stroke:#ffffff,stroke-width:2px,color:#000,font-weight:bold;

%% Réseau interne - Ifremer WIS2 Node
subgraph internal_network["🔗 Ifremer WIS2 Node"]
  broker_ifremer["📨 Ifremer Broker"]:::broker

  %% Serveur Web sécurisé
  subgraph https_server_subgraph["🌐 HTTPS Server"]
    data_filesystem["🗂️ Argo Data Files"]:::filesystem
    diffusion_process["⚙️ Argo Event Processor"]:::process
  end

  %% Planification des tâches - Airflow
  subgraph airflow_subgraph["🗓️ Airflow Scheduler"]
    mqtt_listener["🎧 Event Listener"]:::listener
    notification_message_process["📦 WIS2 Message Generator"]:::process
  end

  %% Connexions internes
  data_filesystem -->|new file| diffusion_process
  diffusion_process -->|CloudEvent/STAC message| broker_ifremer
  broker_ifremer -->|CloudEvent/STAC| mqtt_listener
  mqtt_listener -->|Trigger Notification| notification_message_process
  notification_message_process -->|WIS2 Notification| broker_ifremer
end

%% Noeuds externes
broker_wis2["📨 WIS2 Global Broker"]:::broker

%% Connexions externes
broker_wis2 -->|origin/a/wis2/fr-ifremer-argo/...| broker_ifremer

```

## Organization

The project is structured as follows:

- `broker/`: Directory containing MQTT broker data and configuration.
- `scheduler/`: Directory containing Airflow scheduler data and configuration.
- `data/`: Directory containing test data.
- `compose.yml`: Docker Compose configuration file defining the services required to execute the full notification message publication process.

## Configuration

- `broker/config`: Contains the Mosquitto configuration file.
- `broker/data`: Contains user configurations and topic permissions for Mosquitto.

- `scheduler/config`: Airflow configuration.
- `scheduler/dags`: Airflow DAGs.
- `scheduler/logs`: Airflow logs.
- `scheduler/plugins`: Airflow plugins.
- `scheduler/data`: Airflow externalised DAGs data, to store event / notifications from the DAGs in case of error

## Services

### Metadata

Metadata management is not included in this demonstration since it only needs to be executed once. However, the process follows the same principle. An example of a [JSON **Core Metadata Profile** file](/data/core-metadata/fr-ifremer-argo-core-metadata.json) is stored in the test data directory. This file must be hosted on a web server with a publicly accessible URL. From the **Core Metadata Profile**, a [WIS2 notification message](/data/notification-message/core-metadata-msg-notification.json) must be created and published to the global WIS2 broker on the dedicated topic. For example, for Argo:

```bash
mqttx pub -h localhost --debug -p 8081 -l ws -u wis2-argo-rw -P "wis2-argo-rw" --path / -t origin/a/wis2/fr-ifremer-argo/metadata -m "$(cat ./data/notification-message/core-metadata-msg-notification.json)"
```

### Data

Microservices described in the `compose.yml` files:

1. `broker/compose.yml`: A [Mosquitto](https://devops.ifremer.fr/development/tools/message/mosquitto) microservice implementing the MQTT protocol to transmit events (data file creation) and notifications (WIS2 notification message).
2. `scheduler/compose.yml`: 9 microservices enabling local execution of the Airflow scheduler ([official documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)). Airflow triggers the processing chain (creation of a WIS2 notification message) each time an event is received (new data file creation).
3. `argo-event-diffusion`: A microservice simulating the creation of an Argo file diffusion event.
4. `wis2-argo-subscription`: A microservice simulating the global WIS2 broker, receiving notification messages from Argo.

We the [solution is running properly](#get-started), you can simulate another Argo diffusion event from a message in your file system with [`mqttx` client](https://mqttx.app/downloads?os=linux) :

```bash
mqttx pub -h localhost --debug -p 8081 -l ws -u prod-files-rw -P "prod-files-rw" --path / -t diffusion/files/coriolis/argo/bufr -m "$(cat ./data/event-message/bufr-creation-cloudevent.json)"
```

## Get Started

To simulate the publication of a notification message on a WIS2 broker upon receiving an Argo data file creation event, follow these steps:

- Start the Mosquitto and Airflow microservices and simulate Argo file diffusion using Docker:

```bash
docker compose up
```

- Simulate again Argo file diffusion using Docker:

```bash
docker compose run argo-event-diffusion
```

- Once the process is complete, stop and remove the containers:

```bash
docker compose down
```

- Cleaning up : to stop and delete containers, delete volumes with database data and download images

```bash
docker compose down --volumes --rmi all
```

## Classic Use Case

Once the application is running locally, access the Airflow web interface at <http://localhost:8080> using the default credentials (`airflow` / `airflow`).

Filter the view to show only active DAGs. You will see the two DAGs related to this project:

![Airflow active DAGs](/assets/images/airflow_actives_dags.png)

- `📂 WIS2 - Listen file diffusion event`: This DAG subscribes to file diffusion topics on the MQTT broker. When a file diffusion event is received, it triggers the publication DAG.
- `🔔 WIS2 - Publish notification message`: This DAG is triggered by a diffusion event. It processes, validates, and publishes a WIS2 notification message.

As a demonstration, a first diffusion event is automatically sent when the system becomes ready.

Click on each DAG to inspect the status of individual tasks:

- `📂 WIS2 - Listen file diffusion event` contains a single task that listens on the MQTT broker. If you click on the task, you can access the logs and view the received events.

![Listener DAG status](/assets/images/listener_dag_status.png)

- `🔔 WIS2 - Publish notification message` includes several tasks that handle the generation, validation, and publication of the notification message. You can inspect the logs of each task. In the example below, the message was successfully published:

To simulate another file diffusion event, run the following Docker command:

```bash
docker compose run argo-event-diffusion
```

Alternatively, you can send a CloudEvent message manually using the [`mqttx`](https://mqttx.app/downloads?os=linux) client:

```bash
mqttx pub -h localhost --debug -p 8081 -l ws -u prod-files-rw -P "prod-files-rw" --path / -t diffusion/files/coriolis/argo/bufr -m "$(cat /path-to-data/cloudevents-message.json)"
```

---

## What to Do in Case of an Error?

If an error occurs during the `🔔 WIS2 - Publish notification message` process, a copy of the original event is saved when received. You can find the stored message at:

```text
./scheduler/data/{{dag_id}}/{{run_id}}/{{task_id}}.json
```

### ✅ To fix and retry

You can edit the message and clear the DAG to rerun it.

#### Option 1 – Using the command line

```bash
# Edit the CloudEvent message
vim ./scheduler/data/{{dag_id}}/{{run_id}}/{{task_id}}.json

# Clear and rerun the DAG after editing
docker exec -it wis2-mqtt-broker-airflow-worker-1 airflow tasks clear wis2-publish-message-notification \
  -s "2025-04-15T15:00:47" \
  -e "2025-04-15T15:00:48" \
  --yes
```

💡 **Tip**: To retrieve the exact `dag_run` execution date:

```bash
docker exec -it wis2-mqtt-broker-airflow-worker-1 airflow dags list-runs -d wis2-publish-message-notification
```

#### Option 2 – From the Airflow web interface

1. Edit the message file depending on the error:  
   `./scheduler/data/{{dag_id}}/{{run_id}}/{{task_id}}.json`
2. Open the Airflow UI at [http://localhost:8080](http://localhost:8080)
3. Click on the DAG and locate the failed run:  
   ![Select DAG Failed](/assets/images/select_failed_dag_run.png)
4. Click the **"Clear"** button and choose **"Clear existing tasks"**:  
   ![Clear existing tasks](/assets/images/clear_dag_existing_tasks.png)