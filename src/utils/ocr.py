"""OCR utility for extracting text from scanned PDF pages.

Uses pytesseract (Tesseract OCR) to convert PDF page images to text.
Requires system-level Tesseract installation (``brew install tesseract``).
"""
from __future__ import annotations

from src.utils.logger import get_logger

logger = get_logger(__name__)


def _render_ocr_image(page):
    import pytesseract  # noqa: F401
    from PIL import Image  # noqa: F401

    page_image = page.to_image(resolution=300)
    pil_image = page_image.original

    if pil_image.mode != "L":
        pil_image = pil_image.convert("L")

    return pil_image


def ocr_page_to_text(page, *, config: str = "") -> str:
    """Extract text from a pdfplumber page using OCR.

    Renders the page to a PIL Image at 300 DPI, then runs Tesseract OCR.

    Args:
        page: A pdfplumber page object.

    Returns:
        Extracted text string, or empty string on failure.

    Raises:
        ImportError: If pytesseract or Pillow is not installed.
    """
    import pytesseract

    try:
        pil_image = _render_ocr_image(page)
        text = pytesseract.image_to_string(pil_image, config=config)
        return text or ""

    except Exception as e:
        logger.warning(f"OCR failed for page: {e}")
        return ""


def ocr_page_to_data(page, *, config: str = "--psm 11") -> list[dict]:
    """Extract positioned OCR tokens from a pdfplumber page.

    Returns a list of token dictionaries with ``text``, ``left``, ``top``,
    ``width``, ``height``, and ``conf`` keys.
    """
    import pytesseract

    try:
        pil_image = _render_ocr_image(page)
        data = pytesseract.image_to_data(
            pil_image,
            config=config,
            output_type=pytesseract.Output.DICT,
        )

        tokens = []
        for index, raw_text in enumerate(data["text"]):
            text = raw_text.strip()
            if not text:
                continue

            confidence = data["conf"][index]
            conf = float(confidence) if confidence != "-1" else -1.0
            if conf < 0:
                continue

            tokens.append(
                {
                    "text": text,
                    "left": int(data["left"][index]),
                    "top": int(data["top"][index]),
                    "width": int(data["width"][index]),
                    "height": int(data["height"][index]),
                    "conf": conf,
                }
            )

        return tokens
    except Exception as e:
        logger.warning(f"OCR token extraction failed for page: {e}")
        return []
