import os
import socks
import argparse
import json
from dotenv import load_dotenv
import paho.mqtt.client as mqtt

# Charger le fichier .env
load_dotenv()

# Paramètres du broker
BROKER = os.getenv("MQTT_BROKER_DOMAIN")  # Ou l'adresse de ton broker MQTT
PORT = int(os.getenv("MQTT_BROKER_PORT"))  # Port par défaut pour MQTT
SSL_ENABLED = os.getenv("MQTT_BORKER_SSL_ENABLED")  # activate or not SSL

# Configuration du proxy si nécessaire
PROXY_HOST = os.getenv("PROXY_HOST")
PROXY_PORT = os.getenv("PROXY_PORT")

# définition des topics
# TOPIC_SUB = os.getenv("MQTT_BROKER_TOPIC_SUB")  # Topic pour s'abonner
# TOPIC_PUB = os.getenv("MQTT_BROKER_TOPIC_PUB")  # Topic pour publier

# Récupération du mot de passe depuis l'environnement
USERNAME = os.getenv("MQTT_USERNAME")
PASSWORD = os.getenv("MQTT_PASSWORD")

# Callback lorsque la connexion est établie avec le broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connexion réussie au broker MQTT")
        # client.subscribe(TOPIC_SUB)  # S'abonner à un topic
    else:
        print(f"Échec de connexion, code d'erreur : {rc}")

# Callback lorsqu'un message est reçu
def on_message(client, userdata, msg):
    print(f"Message reçu sur {msg.topic} : {msg.payload.decode()}")

def main():
    """Point d'entrée principal du script."""
    if USERNAME is None:
        raise ValueError("🚨 Erreur : la variable d'environnement MQTT_USERNAME n'est pas définie !")

    if PASSWORD is None:
        raise ValueError("🚨 Erreur : la variable d'environnement MQTT_PASSWORD n'est pas définie !")

    """Point d'entrée principal du script."""
    parser = argparse.ArgumentParser(description="publication sur un broker mosquitto")
    parser.add_argument("topic", help="Topic sur lequel on souhaite publier")
    parser.add_argument("file_path", help="Topic sur lequel on souhaite publier")
    args = parser.parse_args()

    TOPIC_PUB = args.topic  # Topic pour publier
    # TOPIC_SUB = args.topic  # Topic pour souscrire
    try:
        with open(args.file_path) as fh:
            message = json.load(fh)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Erreur lors du chargement du fichier : {e}")
    
    # Création du client MQTT avec WebSockets
    client = mqtt.Client(transport="websockets")  # Activation du mode WebSocket
    # Enable SSL if needed
    if SSL_ENABLED:
        print(f"✅ SSL is enabled")
        client.tls_set()


    if PROXY_HOST and PROXY_PORT:
        print(f"✅ Utilisation du proxy {PROXY_HOST}:{PROXY_PORT}")
        client.proxy_set(proxy_type=socks.HTTP, proxy_addr=PROXY_HOST, proxy_port=int(PROXY_PORT))

    # Ajout des identifiants d'authentification
    client.username_pw_set(USERNAME, PASSWORD)

    # Attacher les callbacks
    client.on_connect = on_connect
    client.on_message = on_message

    # Connexion au broker
    client.connect(BROKER, PORT, 60)

    # Boucle pour maintenir la connexion et écouter les messages
    client.loop_start()

    # Exemple : publier un message
    client.publish(TOPIC_PUB, "test")

    try:
        while True:
            pass  # Boucle infinie pour maintenir la connexion
    except KeyboardInterrupt:
        print("\nDéconnexion du client MQTT")
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()
