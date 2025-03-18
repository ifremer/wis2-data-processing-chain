# Résumé

Ce projet est une démonstration d'une chaine de traitement qui valide puis publie un message sur Le [WMO Information System](https://community.wmo.int/en/activity-areas/wis/wis2-implementation) (WIS 2.0) via un broker MQTT (mosquitto). Le code a été écrit en Python et utilise les bibliothèques suivantes :

- `Paho` : Client broker MQTT
- `pywis_pubsub` : Permet de publier / souscrire à des topics WIS2 et valider des message de notification.
- `pywcmp` : Permet de valider des message de notification pour WMO WIS Core Metadata Profile (WCMP).

## Configuration

Le projet nécessite les fichiers suivants :

- `compose.yml` : fichier de configuration pour Docker Compose qui définit les services à exécuter.
- `scripts/.env` : fichier de configuration pour les variables d'environnement des scripts Python.
- `scripts/*.py` : scripts Python pour valider, publier, souscrire.

## Services

Le projet démarre trois services :

1. **broker** : service Mosquitto qui fonctionne en tant que broker MQTT.
2. **validate-metadata** : service Python qui valide un message de notification JSON de type **Core Metadata Profile** dans le répertoire `/data`.
3. **publish-metadata** : service Python qui publie un un message de notification JSON de type **Core Metadata Profile** sur le topic `origin/a/wis2/fr-ifremer-argo` après validation.
4. **validate-data** : service Python qui valide un message de notification JSON de type données dans le répertoire `/data`.
5. **publish-metadata** : service Python qui publie un message JSON sur le topic `origin/a/wis2/fr-ifremer-argo` après validation.

## Lancement du projet

Pour lancer le projet, exécutez les commandes suivantes dans votre terminal :

```bash
docker compose up --build
```

## Utilisation

Pour valider et publier un message JSON sur un broker MQTT local, placez-le dans le répertoire `/data`, mettez à jour le fichier `compose.yml` pour pointer sur ce fichier et exécutez la commande suivante :

```bash
docker compose up
```
