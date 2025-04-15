import hashlib
import base64
import requests
import multihash


def verify_file_integrity(file_path, expected_hash_base64, method="sha3-256"):
    """Vérifie l'intégrité d'un fichier avec un hash SHA3-256 (ou autre methode) en base64."""

    # Initialiser l'algorithme de hachage
    data_value = hashlib.new(method)

    # Lire le fichier en blocs pour éviter une surcharge mémoire
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            data_value.update(chunk)

    # Calculer le hash et l'encoder en base64
    computed_hash_base64 = base64.b64encode(data_value.digest()).decode()

    # Comparer avec le hash attendu
    if computed_hash_base64 == expected_hash_base64:
        print("✅ Intégrité du fichier vérifiée : Le hash correspond !")
        return True
    else:
        print("❌ Alerte : Le hash du fichier ne correspond pas !")
        print(f"  ➤ Attendu   : {expected_hash_base64}")
        print(f"  ➤ Calculé   : {computed_hash_base64}")
        return False


def generate_checksum_base64(file_path, method="sha3-256"):
    # calcultate checksum's file with sha3-256
    data_value = hashlib.new(method)
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            data_value.update(chunk)
    return base64.b64encode(data_value.digest()).decode()


def generate_multihash_hex(file_path, method="sha3-256"):
    # calcultate checksum's file with sha3-256
    data_value = hashlib.new(method)
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            data_value.update(chunk)
    mh = multihash.encode(data_value.digest(), method)
    return mh.hex()


def generate_multihash_base64_without_prefix(file_path, method="sha3-256"):
    # calcultate checksum's file with sha3-256
    data_value = hashlib.new(method)
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            data_value.update(chunk)
    mh = multihash.encode(data_value.digest(), method)
    decoded = multihash.decode(mh)
    return base64.b64encode(decoded.digest).decode()


def generate_multihash_base64_without_prefix_url(url, method="sha3-256"):
    # Télécharger le fichier
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Vérifier les erreurs HTTP

    # Initialiser l'algorithme de hachage
    data_value = hashlib.new(method)

    # Lire le contenu en blocs et mettre à jour le hash
    for chunk in response.iter_content(4096):
        data_value.update(chunk)

    mh = multihash.encode(data_value.digest(), method)
    decoded = multihash.decode(mh)
    return base64.b64encode(decoded.digest).decode()


def convert_checksum(hex_str, method="sha3-256"):
    hash_bytes = bytes.fromhex(hex_str)  # 🔹 Convertir hex en bytes
    mh = multihash.encode(hash_bytes, method)  # 🔥 Encapsuler avec Multihash
    decoded = multihash.decode(mh)
    mh_base64 = base64.b64encode(decoded.digest).decode()

    print("🔢 Hash Multihash (hex) :", hex_str)
    print("🔢 Hash en base64 :", mh_base64)
    return mh_base64


# Exemple d'utilisation avec SHA3-256
file_path = "/home/lbruvryl/development/sources/gitlab.ifremer.fr/amrit/development/wis2-mqtt-broker/data/event-message/IOPX01_LFVX_071528.json"

multihash_hex = generate_multihash_hex(file_path)
multihash_base64 = generate_checksum_base64(file_path)
hash_base64 = generate_multihash_base64_without_prefix(file_path)
# hash_base64_url = generate_multihash_base64_without_prefix_url("https://data-argo.ifremer.fr/etc/bufr/202504/IOPX01_LFVX_071528.BU")

print(f"from STAC : {multihash_hex}")
print(f"to WIS2 : {hash_base64}")

hash_bytes = bytes.fromhex(multihash_hex)  # 🔹 Convertir hex en bytes
decoded = multihash.decode(hash_bytes)
print(decoded.digest.hex())
print(base64.b64encode(decoded.digest).decode())
# Récupérer le nom de la méthode
# hash_method = multihash.coerce_code(decoded.code)
hash_method = multihash.constants.CODE_HASHES[decoded.code]
print(hash_method)
expected_hash_base64 = base64.b64encode(decoded.digest).decode()



# hash_converted_base64 = convert_checksum(hash_hex)


# print(hash_converted_base64)

verify_file_integrity(file_path, expected_hash_base64, method="sha3_256")
