#!/usr/bin/env python3
"""Bank Statement Processor — processes PDF bank statements through a 4-stage pipeline."""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import List

from src.models.database import init_db
from src.pipeline.ai_classifier import AIClassifierStage
from src.pipeline.data_cleanser import DataCleanserStage
from src.pipeline.pdf_extractor import PDFExtractorStage
from src.pipeline.queue import Pipeline, PipelineContext
from src.pipeline.regex_classifier import RegexClassifierStage, seed_classification_rules
from src.profiles import BankProfileFactory
from src.utils.logger import get_logger

logger = get_logger(__name__)

DEFAULT_CATEGORIES = [
    "Groceries",
    "Utilities",
    "Rent/Mortgage",
    "Salary",
    "Transfer",
    "Subscriptions",
    "Transport",
    "Dining",
    "Entertainment",
    "Healthcare",
    "Insurance",
    "Cash Withdrawal",
    "Clothing/Apparel",
    "Electronics/Home",
    "Education",
    "Charity",
    "Other",
]


def build_pipeline(
    session_factory,
    rules_path: str,
    api_key: str | None,
    dry_run: bool,
    bank: str | None = None,
) -> Pipeline:
    """Build the processing pipeline with all stages."""
    if bank:
        profile = BankProfileFactory.get(bank)
        extractor = PDFExtractorStage(profile=profile, auto_detect=False)
        logger.info(f"Using bank profile: {profile.name}")
    else:
        extractor = PDFExtractorStage()  # auto-detect from PDF

    stages = [
        extractor,
        DataCleanserStage(session_factory),
        RegexClassifierStage(rules_path, session_factory),
    ]

    if not dry_run and api_key:
        stages.append(
            AIClassifierStage(
                api_key=api_key,
                rules_path=rules_path,
                session_factory=session_factory,
                categories=DEFAULT_CATEGORIES,
            )
        )
    elif dry_run:
        logger.info("Dry run mode — AI classification stage skipped")
    elif not api_key:
        logger.warning(
            "ANTHROPIC_API_KEY not set — AI classification stage skipped"
        )

    return Pipeline(stages)


def _safe_move(src: Path, dest_dir: Path) -> Path:
    """Move *src* into *dest_dir*, adding a timestamp suffix if a file with the same name already exists."""
    dest = dest_dir / src.name
    if dest.exists():
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = dest_dir / f"{src.stem}_{stamp}{src.suffix}"
    shutil.move(str(src), str(dest))
    return dest


def process_files(
    pdf_files: List[Path],
    pipeline: Pipeline,
    input_dir: Path,
) -> List[PipelineContext]:
    """Run the pipeline on each PDF, moving files to processed/ or failed/."""
    processed_dir = input_dir / "processed"
    failed_dir = input_dir / "failed"
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(failed_dir, exist_ok=True)

    results: List[PipelineContext] = []
    for pdf_path in pdf_files:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {pdf_path}")
        logger.info(f"{'='*60}")

        ctx = PipelineContext(file_path=str(pdf_path))
        try:
            result = pipeline.run(ctx)
            results.append(result)
            _safe_move(pdf_path, processed_dir)
            logger.info(f"Moved to processed: {pdf_path.name}")
        except Exception as e:
            logger.error(f"Failed to process {pdf_path}: {e}")
            _safe_move(pdf_path, failed_dir)
            logger.info(f"Moved to failed: {pdf_path.name}")
            continue

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Process PDF bank statements through extraction, cleansing, and classification."
    )
    parser.add_argument(
        "--pdf-dir",
        type=str,
        default="data",
        help="Directory containing PDF bank statements (default: data)",
    )
    parser.add_argument(
        "--pdf-file",
        type=str,
        default=None,
        help="Process a single PDF file instead of a directory",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="data/statements.db",
        help="Path to the SQLite database file (default: data/statements.db)",
    )
    parser.add_argument(
        "--rules-path",
        type=str,
        default="config/classification_rules.json",
        help="Path to the classification rules JSON file (default: config/classification_rules.json)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip the AI classification stage",
    )
    parser.add_argument(
        "--bank",
        type=str,
        default=None,
        help=(
            "Bank profile to use for PDF parsing. "
            "Available: absa, fnb, nedbank, standard_bank, capitec. "
            "If not specified, the bank is auto-detected from the PDF."
        ),
    )

    args = parser.parse_args()

    # Resolve paths
    db_path = os.path.abspath(args.db_path)
    rules_path = os.path.abspath(args.rules_path)

    # Ensure data directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Initialize database
    session_factory = init_db(db_path)
    logger.info(f"Database initialized at: {db_path}")

    # Seed classification rules into database (no-op if already populated)
    seed_classification_rules(session_factory, rules_path)

    # Get API key
    api_key = os.environ.get("ANTHROPIC_API_KEY")

    # Build pipeline
    pipeline = build_pipeline(
        session_factory, rules_path, api_key, args.dry_run, args.bank
    )

    # Collect PDF files
    if args.pdf_file:
        pdf_files = [Path(args.pdf_file)]
    else:
        pdf_dir = Path(args.pdf_dir)
        pdf_files = sorted(pdf_dir.glob("*.pdf"))

    if not pdf_files:
        logger.error("No PDF files found to process")
        sys.exit(1)

    logger.info(f"Found {len(pdf_files)} PDF file(s) to process")

    # Determine input directory for file management
    if args.pdf_file:
        input_dir = Path(args.pdf_file).parent
    else:
        input_dir = Path(args.pdf_dir)

    # Process each PDF (moves to processed/ or failed/)
    results = process_files(pdf_files, pipeline, input_dir)

    # Print summary
    logger.info(f"\n{'='*60}")
    logger.info("PROCESSING COMPLETE")
    logger.info(f"{'='*60}")
    for result in results:
        summary = result.summary()
        logger.info(
            f"  {summary['file']}: "
            f"{summary['classified']} classified, "
            f"{summary['unclassified']} unclassified, "
            f"{summary['errors']} warnings"
        )


if __name__ == "__main__":
    main()
