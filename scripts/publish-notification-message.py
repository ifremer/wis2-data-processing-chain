import os
import socks
import argparse
import json
import time
from dotenv import load_dotenv
import paho.mqtt.client as mqtt

# Charger le fichier .env
load_dotenv()

# Paramètres du broker
BROKER = os.getenv("MQTT_BROKER_DOMAIN")
PORT = int(os.getenv("MQTT_BROKER_PORT", 9001))  # Valeur par défaut : 9001
SSL_ENABLED = os.getenv("MQTT_BORKER_SSL_ENABLED", "false").lower() == "true"

# Configuration du proxy si nécessaire
PROXY_HOST = os.getenv("PROXY_HOST")
PROXY_PORT = os.getenv("PROXY_PORT")

# Récupération des identifiants
USERNAME = os.getenv("MQTT_USERNAME")
PASSWORD = os.getenv("MQTT_PASSWORD")

# Vérifier les identifiants MQTT
if not USERNAME or not PASSWORD:
    raise ValueError("🚨 Erreur : MQTT_USERNAME et MQTT_PASSWORD doivent être définis dans le fichier .env !")

# Callback lors de la connexion au broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connexion réussie au broker MQTT")
    else:
        print(f"❌ Échec de connexion, code d'erreur : {rc}")

# Callback lors de la réception d'un message
def on_message(client, userdata, msg):
    print(f"📩 Message reçu sur {msg.topic} : {msg.payload.decode()}")

# Fonction pour charger le fichier JSON
def load_json_file(file_path):
    """Charge un fichier JSON et retourne son contenu."""
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print(f"❌ Erreur : Fichier '{file_path}' non trouvé.")
    except json.JSONDecodeError as e:
        print(f"❌ Erreur de format JSON : {e}")
    return None

# Fonction principale
def main():
    """Point d'entrée principal du script."""
    parser = argparse.ArgumentParser(description="Publier un fichier JSON sur un broker MQTT via WebSockets")
    parser.add_argument("topic", help="Topic MQTT sur lequel publier les données")
    parser.add_argument("file_path", help="Chemin du fichier JSON à publier")
    args = parser.parse_args()

    # Charger le fichier JSON
    json_data = load_json_file(args.file_path)
    if json_data is None:
        return  # Quitte le script si le fichier JSON est invalide

    # Création du client MQTT avec WebSockets
    client = mqtt.Client(transport="websockets")

    # Configuration SSL si activé
    if SSL_ENABLED:
        print("✅ SSL activé pour la connexion MQTT")
        client.tls_set()

    # Configuration du proxy si nécessaire
    if PROXY_HOST and PROXY_PORT:
        print(f"🔗 Connexion via proxy {PROXY_HOST}:{PROXY_PORT}")
        client.proxy_set(proxy_type=socks.HTTP, proxy_addr=PROXY_HOST, proxy_port=int(PROXY_PORT))

    # Ajout des identifiants d'authentification
    client.username_pw_set(USERNAME, PASSWORD)

    # Attacher les callbacks
    client.on_connect = on_connect
    client.on_message = on_message

    # Connexion au broker
    print(f"🔗 Connexion au broker MQTT : {BROKER}:{PORT}...")
    try:
        client.connect(BROKER, PORT, 60)
        client.loop_start()
    except Exception as e:
        print(f"❌ Impossible de se connecter au broker MQTT : {e}")
        return

    # Publier le fichier JSON sous forme de message MQTT
    message = json.dumps(json_data, ensure_ascii=False)
    client.publish(args.topic, message)
    print(f"📤 Message envoyé → {args.topic} : {message}")

    # Attendre un court instant pour s'assurer que le message est bien envoyé
    time.sleep(2)

    # Déconnexion propre
    client.loop_stop()
    client.disconnect()
    print("✅ Déconnexion du client MQTT.")

if __name__ == "__main__":
    main()
