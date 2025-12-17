import base64
import uuid
import logging
from urllib.parse import urlparse
from datetime import datetime, timezone

import multihash
import pystac

logger = logging.getLogger(__name__)


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
                    "pubtime": datetime.now(timezone.utc).isoformat(),
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
