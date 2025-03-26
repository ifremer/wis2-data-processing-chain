import os
import json
import paho.mqtt.client as mqtt


# Paramètres du broker
MQTT_BROKER = os.getenv("MQTT_BROKER_DOMAIN")
MQTT_PORT = int(os.getenv("MQTT_BROKER_PORT"))
MQTT_TOPIC = os.getenv("MQTT_BROKER_TOPIC")
SSL_ENABLED = os.getenv("MQTT_BORKER_SSL_ENABLED", "false").lower() == "true"

# Configuration du proxy si nécessaire
PROXY_HOST = os.getenv("PROXY_HOST")
PROXY_PORT = os.getenv("PROXY_PORT")

# Récupération des identifiants
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")


# Callback lorsque la connexion est établie avec le broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connexion réussie au broker MQTT")
        client.subscribe(MQTT_TOPIC)  # S'abonner à un topic
    else:
        print(f"Échec de connexion, code d'erreur : {rc}")


# Callback exécuté lorsqu'un message est reçu
def on_message(client, userdata, message):
    payload = message.payload.decode("utf-8")
    data = json.loads(payload)
    print(f"Message reçu: {data}")
    return data  # Peut être stocké dans XCom pour d'autres tâches Airflow


# Fonction principale
def main():
    """Point d'entrée principal du script."""
    # Création du client MQTT avec WebSockets
    client = mqtt.Client(transport="websockets")

    # Configuration SSL si activé
    if SSL_ENABLED:
        print("✅ SSL activé pour la connexion MQTT")
        client.tls_set()

    # Ajout des identifiants d'authentification
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    # Attacher les callbacks
    client.on_connect = on_connect
    client.on_message = on_message

    # Connexion au broker
    client.connect(MQTT_BROKER, MQTT_PORT, 60)

    print(f"Écoute du topic {MQTT_TOPIC}...")
    client.loop_forever()  # Bloque l'exécution et écoute en continu


if __name__ == "__main__":
    main()