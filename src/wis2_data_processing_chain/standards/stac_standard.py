import logging
import pystac

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_stac_item(stac_item: dict) -> dict:
    """Compute a base64-encoded integrity hash from a multihash value."""

    try:
        stac_item = pystac.Item.from_dict(stac_item)
        stac_item.validate()
        logger.info("✅ STAC Item format is valid !")
    except Exception:
        logger.exception("❌ STAC Item format is not valid !")
        raise
