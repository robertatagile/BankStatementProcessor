"""OCR utility for extracting text from scanned PDF pages.

Uses pytesseract (Tesseract OCR) to convert PDF page images to text.
Requires system-level Tesseract installation (``brew install tesseract``).
"""
from __future__ import annotations

from src.utils.logger import get_logger

logger = get_logger(__name__)


def ocr_page_to_text(page) -> str:
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
    from PIL import Image

    try:
        # Render page to image at 300 DPI for good OCR quality
        page_image = page.to_image(resolution=300)
        pil_image = page_image.original

        # Convert to grayscale for better OCR accuracy
        if pil_image.mode != "L":
            pil_image = pil_image.convert("L")

        # Run Tesseract OCR
        text = pytesseract.image_to_string(pil_image)
        return text or ""

    except Exception as e:
        logger.warning(f"OCR failed for page: {e}")
        return ""
