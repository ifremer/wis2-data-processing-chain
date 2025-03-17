import json
from datetime import datetime, timezone

from pywis_pubsub.mqtt import MQTTPubSubClient
from pywis_pubsub.publish import create_message

def pretty_print_json(data, title="Résultat"):
    """Affiche un dictionnaire JSON formaté proprement."""
    print(f"\n=== {title} ===")
    print(json.dumps(data, indent=4, ensure_ascii=False))


def create_message(data):
    """Exécute les tests ETS et affiche les résultats formatés."""
    return create_message(
        topic='origin/a/wis2/fr-ifremer-argo',
        content_type='application/x-bufr',
        url='http://www.meteo.xx/stationXYZ-20221111085500.bufr4',
        identifier='stationXYZ-20221111085500',
        datetime_=datetime.now(timezone.utc),
        geometry=[33.8, -11.8, 123],
        metadata_id='x-urn:wmo:md:test-foo:htebmal2001',
        wigos_station_identifier='0-20000-12345',
        operation='update'
)


def main():
    """Point d'entrée principal du script."""
    topic = 'origin/a/wis2/fr-ifremer-argo'
    m = MQTTPubSubClient('ws://localhost:8080')
    m.pub(topic, json.dumps(create_message(topic)))


if __name__ == "__main__":
    main()
