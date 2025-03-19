# Résumé

Ce projet est une démonstration d'une chaine de traitement qui valide puis publie un message sur Le [WMO Information System](https://community.wmo.int/en/activity-areas/wis/wis2-implementation) (WIS 2.0) via un broker MQTT (mosquitto). Le code a été écrit en Python et utilise les bibliothèques suivantes :

- `Paho` : Client broker MQTT Python pour publier et souscire aux topics.
- `pywis_pubsub` : Permet de valider des message de notification type data.
- `pywcmp` : Permet de valider des message de notification type WMO WIS Core Metadata Profile (WCMP).

## Configuration

Le projet nécessite les fichiers suivants :

- `compose.yml` : fichier de configuration pour Docker Compose qui définit les services à exécuter.
- `scripts/*.py` : scripts Python pour valider, publier, souscrire.

## Services

Le projet démarre 5 services :

1. **broker** : service Mosquitto qui fonctionne en tant que broker MQTT.
2. **validate-metadata** : service Python qui valide fichier JSON de type **Core Metadata Profile** dans le répertoire `/data`.
3. **validate-metadata-message** : service Python qui valide un message de notification à envoyer pour le fichier de métadonnées.
4. **validate-data-message** : service Python qui valide un message de notification pour un fichier de données dans le répertoire `/data`.

5. **publish-metadata** : service Python qui publie un message de notification pour les fichier de métadonnées sur le topic `origin/a/wis2/fr-ifremer-argo/metadata` après validation.
6. **publish-metadata** : service Python qui publie un message de notification pou run fichier de données sur le topic `origin/a/wis2/fr-ifremer-argo/core/data/ocean/surface-based-observations/drifting-ocean-profilers` après validation.

## Utilisation

Pour valider et publier un message JSON sur un broker MQTT local, placez-le dans le répertoire `/data`, mettez à jour le fichier `compose.yml` pour pointer sur ce fichier et exécutez la commande suivante :

```bash
docker compose up --build
```

supression des conteneurs :

```bash
docker compose down
```
