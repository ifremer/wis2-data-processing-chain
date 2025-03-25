import os
import json
import uuid
import hashlib
import argparse
from datetime import datetime, timezone


def compute_sha512(file_path):
    """Calcule le hash SHA-512 d'un fichier."""
    sha512 = hashlib.sha512()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha512.update(chunk)
    return sha512.hexdigest()


def get_file_path_id(file_path, depth):
    """Construit file_path_id en gardant les N derniers niveaux du chemin."""
    parts = file_path.strip(os.sep).split(os.sep)
    return os.path.join(*parts[-depth:])


def generate_json_message(file_path, base_url, depth=2, output_file=None):
    """Génère un message JSON basé sur un fichier.

    - Si `output_file` est spécifié, écrit le JSON dans ce fichier.
    - Sinon, retourne une chaîne JSON.
    """

    file_path_id = get_file_path_id(file_path, depth)
    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    file_mod_time = datetime.fromtimestamp(
        os.path.getmtime(file_path), timezone.utc
    ).isoformat()

    sha512_hash = compute_sha512(file_path)

    # Exemple d'ID et d'URN, à adapter selon tes besoins
    data_id = f"wis2/fr-ifremer-argo/core/data/{file_path_id}"
    metadata_id = "urn:wmo:md:fr-ifremer-argo:cor:msg:argo"

    # Génération du message JSON
    message = {
        "id": str(uuid.uuid4()),
        "conformsTo": ["http://wis.wmo.int/spec/wnm/1/conf/core"],
        "type": "Feature",
        "geometry": None,
        "properties": {
            "data_id": data_id,
            "metadata_id": metadata_id,
            "pubtime": datetime.now(timezone.utc).isoformat(),
            "integrity": {"method": "sha512", "value": sha512_hash},
            "datetime": file_mod_time,
        },
        "links": [
            {
                "href": f"{base_url}/{file_path_id}/{file_name}",
                "rel": "canonical",
                "type": "application/bufr",
                "length": file_size,
            }
        ],
    }

    if output_file:
        # Écriture du JSON dans un fichier
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(message, f, indent=4)
        return f"JSON sauvegardé dans {output_file}"
    else:
        # Retourner le JSON sous forme de chaîne
        return json.dumps(message, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Génère un notification message WIS2 au format JSON à partir d'un fichier."
    )
    parser.add_argument("file_path", help="Chemin du fichier d'entrée")
    parser.add_argument(
        "--base_url",
        default="https://data-argo.ifremer.fr",
        help="Base URL pour le lien du fichier",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=3,
        help="Nombre de niveaux de dossiers à inclure dans file_path_id",
    )
    parser.add_argument("--output", help="Fichier de sortie pour le JSON (optionnel)")

    args = parser.parse_args()

    result = generate_json_message(
        args.file_path, args.base_url, args.depth, args.output
    )
    print(result)
