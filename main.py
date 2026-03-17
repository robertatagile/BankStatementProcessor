#!/usr/bin/env python3
"""Bank Statement Processor — processes PDF bank statements through a 4-stage pipeline."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from src.models.database import init_db
from src.pipeline.ai_classifier import AIClassifierStage
from src.pipeline.data_cleanser import DataCleanserStage
from src.pipeline.pdf_extractor import PDFExtractorStage
from src.pipeline.queue import Pipeline, PipelineContext
from src.pipeline.regex_classifier import RegexClassifierStage
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
    "Shopping",
    "Education",
    "Charity",
    "Other",
]


def build_pipeline(
    session_factory,
    rules_path: str,
    api_key: str | None,
    dry_run: bool,
) -> Pipeline:
    """Build the processing pipeline with all stages."""
    stages = [
        PDFExtractorStage(),
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

    args = parser.parse_args()

    # Resolve paths
    db_path = os.path.abspath(args.db_path)
    rules_path = os.path.abspath(args.rules_path)

    # Ensure data directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Initialize database
    session_factory = init_db(db_path)
    logger.info(f"Database initialized at: {db_path}")

    # Get API key
    api_key = os.environ.get("ANTHROPIC_API_KEY")

    # Build pipeline
    pipeline = build_pipeline(session_factory, rules_path, api_key, args.dry_run)

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

    # Process each PDF
    results = []
    for pdf_path in pdf_files:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {pdf_path}")
        logger.info(f"{'='*60}")

        ctx = PipelineContext(file_path=str(pdf_path))
        try:
            result = pipeline.run(ctx)
            results.append(result)
        except Exception as e:
            logger.error(f"Failed to process {pdf_path}: {e}")
            continue

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
