import base64
import hashlib
import uuid
import logging
from urllib.parse import urlparse
from pathlib import Path
from datetime import datetime, timezone

import multihash
import pystac

logger = logging.getLogger(__name__)


def to_rfc3339_z(dt: datetime) -> str:
    return (
        dt.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def compute_multihash_integrity(multihash_hex: str) -> dict:
    """Compute a base64-encoded integrity hash from a multihash value."""
    try:
        hash_bytes = bytes.fromhex(multihash_hex)
        decoded = multihash.decode(hash_bytes)
        hash_method = multihash.constants.CODE_HASHES[decoded.code]
        base64_code = base64.b64encode(decoded.digest).decode()
        return {"method": hash_method, "value": base64_code}
    except Exception as e:
        logger.error(f"❌ Error computing multihash integrity: {e}", exc_info=True)
        raise


def compute_sha512_base64(content: bytes) -> str:
    return base64.b64encode(hashlib.sha512(content).digest()).decode("ascii")


def get_url_last_n_segments(url: str, n: int) -> str:
    """Extract the last N segments from a given URL."""
    parsed = urlparse(url)
    parts = parsed.path.strip("/").split("/")
    return "/".join(parts[-n:])


def generate_notification_message_from_stac(stac_item_json: dict) -> dict:
    """Generate a WIS2 notification message from a STAC item."""
    try:
        metadata_id = "urn:wmo:md:fr-ifremer-argo:cor:msg:argo"
        stac_item = pystac.Item.from_dict(stac_item_json)

        for asset_key, asset in stac_item.assets.items():
            logger.info(f"📂 Processing asset: {asset_key}")
            file_id = get_url_last_n_segments(asset.href, 3)
            data_id = f"wis2/fr-ifremer-argo/core/data/{file_id}"
            wis2_integrity = compute_multihash_integrity(
                asset.extra_fields.get("file:checksum")
            )

            message = {
                "id": str(uuid.uuid4()),
                "conformsTo": ["http://wis.wmo.int/spec/wnm/1/conf/core"],
                "type": "Feature",
                "geometry": stac_item.geometry,
                "properties": {
                    "data_id": data_id,
                    "metadata_id": metadata_id,
                    "pubtime": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    ),
                    "integrity": wis2_integrity,
                    "datetime": stac_item.properties.get("datetime"),
                },
                "links": [
                    {
                        "href": asset.href,
                        "rel": "canonical",
                        "type": asset.media_type,
                        "length": asset.extra_fields.get("file:size"),
                    }
                ],
            }

        return message
    except Exception:
        logger.exception("❌ Error building WIS2 message.")
        raise


def generate_notification_message_from_metadata(content_str: str) -> dict:
    # content_bytes = content_str.encode("utf-8")
    content_bytes = base64.b64decode(content_str)

    integrity_value = compute_sha512_base64(content_bytes)
    length = len(content_bytes)

    now = to_rfc3339_z(datetime.now(timezone.utc))

    return {
        "id": str(uuid.uuid4()),
        "conformsTo": ["http://wis.wmo.int/spec/wnm/1/conf/core"],
        "type": "Feature",
        "geometry": None,
        "properties": {
            "data_id": "wis2/fr-ifremer-argo/metadata/core/fr-ifremer-argo-core-metadata.json",
            "metadata_id": "urn:wmo:md:fr-ifremer-argo:cor:msg:argo",
            "pubtime": now,
            "integrity": {
                "method": "sha512",
                "value": integrity_value,
            },
            "datetime": now,
        },
        "links": [
            {
                "href": "https://data-argo.ifremer.fr/etc/wis2/fr-ifremer-argo-core-metadata.json",
                "rel": "update",
                "type": "application/json",
                "length": length,
            }
        ],
    }


def generate_notification_message_from_file(file_path: str) -> dict:
    path = Path(file_path)

    content_bytes = path.read_bytes()

    integrity_value = compute_sha512_base64(content_bytes)
    length = len(content_bytes)

    now = to_rfc3339_z(datetime.now(timezone.utc))
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    pubtime = to_rfc3339_z(mtime)

    return {
        "id": str(uuid.uuid4()),
        "conformsTo": ["http://wis.wmo.int/spec/wnm/1/conf/core"],
        "type": "Feature",
        "geometry": None,
        "properties": {
            "data_id": "wis2/fr-ifremer-argo/metadata/core/fr-ifremer-argo-core-metadata.json",
            "metadata_id": "urn:wmo:md:fr-ifremer-argo:cor:msg:argo",
            "pubtime": pubtime,
            "integrity": {
                "method": "sha512",
                "value": integrity_value,
            },
            "datetime": now,
        },
        "links": [
            {
                "href": "https://data-argo.ifremer.fr/etc/wis2/fr-ifremer-argo-core-metadata.json",
                "rel": "update",
                "type": "application/json",
                "length": length,
            }
        ],
    }
