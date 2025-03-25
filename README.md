# Résumé

Ce projet est une démonstration d'une chaine de traitement qui valide puis publie un message sur Le [WMO Information System](https://community.wmo.int/en/activity-areas/wis/wis2-implementation) (WIS 2.0) via un broker MQTT (mosquitto). Le code a été écrit en Python et utilise les bibliothèques suivantes :

- `Paho` : Client broker MQTT Python pour publier et souscire aux topics.
- `pywis_pubsub` : Permet de valider des message de notification type data.
- `pywcmp` : Permet de valider des message de notification type WMO WIS Core Metadata Profile (WCMP).

## Configuration

Le projet nécessite les fichiers suivants :

- `compose.yml` : fichier de configuration Docker Compose définissant les services à éxécuter pour dérouler toute la chaine de publication d'un message de notification pour un fichier de données.
- `compose.metadata.yml` : fichier de configuration Docker Compose définissant les services à éxécuter pour dérouler toute la chaine de publication d'un message de notification pour les fichier de métadonnées déclarant un dataset (**Core Metadata Profile**).
- `scripts/*.py` : scripts Python pour créer, valider et publier un message de notification.

## Services

### Data

5 microservices :

1. **broker** : service Mosquitto qui fonctionne en tant que broker MQTT.
2. **create-data-message** : service Python qui créé un message de notification à partir d'un fichier de données dans le répertoire `/data`.
3. **validate-data-message** : service Python qui valide un message de notification pour un fichier de données dans le répertoire `/data`.
4. **publish-data-message** : service Python qui publie un message de notification pour un fichier de données sur le topic `origin/a/wis2/fr-ifremer-argo/core/data/ocean/surface-based-observations/drifting-ocean-profilers`.

## Metadata

Le projet démarre 5 services :

1. **broker** : service Mosquitto qui fonctionne en tant que broker MQTT.
2. **validate-metadata** : service Python qui valide fichier JSON de type **Core Metadata Profile** dans le répertoire `/data`.
3. **create-metadata-message** : service Python qui valide fichier JSON de type **Core Metadata Profile** dans le répertoire `/data`.
4. **validate-metadata-message** : service Python qui valide un message de notification à envoyer pour le fichier de métadonnées.
5. **publish-metadata** : service Python qui publie un message de notification pour les fichier de métadonnées sur le topic `origin/a/wis2/fr-ifremer-argo/metadata` après validation.

## Utilisation

Pour valider et publier un message JSON sur un broker MQTT local, placez-le dans le répertoire `/data`, mettez à jour le fichier `compose.yml` pour pointer sur ce fichier et exécutez la commande suivante :

```bash
docker compose up --build
```

supression des conteneurs :

```bash
docker compose down
```

## Notes

### Messages

Fichier de metadonnées à déposer sur le serveur apache contenant les données puis envoyer un message de notification sur : origin/a/wis2/fr-ifremer-argo/metadata

Fichier de message de notification

id : générer un uuid
pubtime: date génération du fichier
metadata_id : identifiant unique après urn:wmo:md:fr-ifremer-argo ?
intégrity : hashage du fichier
datetime : date de l'observation
link :
 type : specifique pour netcdf ? pour buffer :
 taille : a setter proprement

Connexion au broker de test par mail

