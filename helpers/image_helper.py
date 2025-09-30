import tempfile
import os
from PIL import Image

def validate_image(path: str) -> bool:
    try:
        with Image.open(path) as img:
            img.verify()  # Raises error if image is corrupted
        return True
    except Exception:
        return False

def save_fig_to_temp(fig_or_bytes, logger):
    """
    Accept either a matplotlib Figure or raw PNG bytes and save to temp file.
    Validates that the image is readable.
    Returns the temp file path.
    """
    try:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
        tmp_path = tmp.name

        # Save image
        if isinstance(fig_or_bytes, (bytes, bytearray)):
            tmp.write(fig_or_bytes)
            tmp.flush()
            tmp.close()
        else:
            fig_or_bytes.savefig(tmp_path, bbox_inches='tight')
            tmp.close()

        # Validate image
        if not validate_image(tmp_path):
            logger.error("Saved image is invalid or corrupted.")
            os.remove(tmp_path)
            raise ValueError("Image validation failed")

        logger.info(f"Image saved and validated at {tmp_path}")
        return tmp_path

    except Exception as e:
        logger.error(f"Error saving figure to temp file: {e}")
        raise