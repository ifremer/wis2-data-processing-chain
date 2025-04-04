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
