from __future__ import annotations

from decimal import Decimal

from sqlalchemy.orm import sessionmaker

from src.models.database import Statement, StatementInfo, StatementLine
from src.pipeline.queue import PipelineContext, Stage
from src.utils.logger import get_logger

logger = get_logger(__name__)

BALANCE_TOLERANCE = Decimal("0.01")


class DataCleanserStage(Stage):
    """Stage 2: Deduplicate, validate totals, and insert records into the database."""

    def __init__(self, session_factory: sessionmaker):
        self._session_factory = session_factory

    def process(self, context: PipelineContext) -> PipelineContext:
        original_count = len(context.raw_lines)

        # Step 0: Filter out lines with missing essential fields
        context.raw_lines = self._filter_incomplete(context.raw_lines)
        filtered_count = original_count - len(context.raw_lines)
        if filtered_count > 0:
            logger.info(f"Filtered {filtered_count} incomplete records (missing date or amount)")

        # Step 1: Deduplicate
        pre_dedup = len(context.raw_lines)
        context.raw_lines = self._deduplicate(context.raw_lines)
        dedup_count = pre_dedup - len(context.raw_lines)
        if dedup_count > 0:
            logger.info(f"Removed {dedup_count} duplicate records")

        # Step 2: Validate totals
        self._validate_totals(context)

        # Step 3: Insert into database
        context.statement_id = self._insert_records(context)

        logger.info(
            f"Inserted statement #{context.statement_id} with "
            f"{len(context.unclassified_lines)} lines"
        )
        return context

    def _filter_incomplete(self, lines: list[dict]) -> list[dict]:
        """Remove lines that are missing a date or amount (cannot be stored)."""
        valid = []
        for line in lines:
            if line.get("date") is None or line.get("amount") is None:
                logger.debug(
                    f"Skipping incomplete line: {line.get('description', '?')}"
                )
                continue
            valid.append(line)
        return valid

    def _deduplicate(self, lines: list[dict]) -> list[dict]:
        """Remove duplicate records based on (date, description, amount)."""
        seen = set()
        unique_lines = []

        for line in lines:
            key = (
                str(line.get("date")),
                line.get("description", "").strip(),
                str(line.get("amount")),
            )
            if key not in seen:
                seen.add(key)
                unique_lines.append(line)
            else:
                logger.debug(f"Duplicate removed: {key}")

        return unique_lines

    def _validate_totals(self, context: PipelineContext) -> None:
        """Validate that transaction totals reconcile with statement balances."""
        opening = context.raw_header.get("opening_balance", Decimal("0.00"))
        closing = context.raw_header.get("closing_balance", Decimal("0.00"))

        if not isinstance(opening, Decimal):
            opening = Decimal(str(opening))
        if not isinstance(closing, Decimal):
            closing = Decimal(str(closing))

        total_credits = Decimal("0.00")
        total_debits = Decimal("0.00")

        for line in context.raw_lines:
            amount = line.get("amount")
            if amount is None:
                continue
            if not isinstance(amount, Decimal):
                try:
                    amount = Decimal(str(amount))
                except Exception:
                    continue

            if line.get("transaction_type") == "credit":
                total_credits += amount
            else:
                total_debits += amount

        expected_change = closing - opening
        actual_change = total_credits - total_debits

        difference = abs(expected_change - actual_change)

        logger.info(
            f"Balance validation: opening={opening}, closing={closing}, "
            f"credits={total_credits}, debits={total_debits}, "
            f"expected_change={expected_change}, actual_change={actual_change}"
        )

        if difference > BALANCE_TOLERANCE:
            warning = (
                f"Balance mismatch: expected change {expected_change}, "
                f"actual change {actual_change} (difference: {difference})"
            )
            logger.warning(warning)
            context.errors.append(warning)
        else:
            logger.info("Balance validation passed")

    def _insert_records(self, context: PipelineContext) -> int:
        """Insert the statement and its lines into the database."""
        header = context.raw_header

        statement = Statement(
            bank_name=header.get("bank_name", "Unknown"),
            account_number=header.get("account_number", "Unknown"),
            statement_date=header.get("period_end") or header.get("period_start"),
            opening_balance=header.get("opening_balance", Decimal("0.00")),
            closing_balance=header.get("closing_balance", Decimal("0.00")),
            file_path=context.file_path,
        )

        with self._session_factory() as session:
            session.add(statement)
            session.flush()  # Get the statement ID

            # Insert personal/address info if present
            if header.get("account_holder") or header.get("address_line1"):
                info = StatementInfo(
                    statement_id=statement.id,
                    account_holder=header.get("account_holder"),
                    address_line1=header.get("address_line1"),
                    address_line2=header.get("address_line2"),
                    address_line3=header.get("address_line3"),
                    postal_code=header.get("postal_code"),
                    account_type=header.get("account_type"),
                    branch_code=header.get("branch_code"),
                )
                session.add(info)

            for line_data in context.raw_lines:
                stmt_line = StatementLine(
                    statement_id=statement.id,
                    date=line_data["date"],
                    description=line_data["description"],
                    amount=line_data["amount"],
                    balance=line_data.get("balance"),
                    transaction_type=line_data.get("transaction_type", "debit"),
                    category=None,
                    classification_method=None,
                )
                session.add(stmt_line)

            session.commit()

            # Populate unclassified_lines with DB IDs for downstream stages
            context.unclassified_lines = [
                {
                    "id": stmt_line.id,
                    "description": stmt_line.description,
                    "amount": stmt_line.amount,
                    "transaction_type": stmt_line.transaction_type,
                    "date": stmt_line.date,
                }
                for stmt_line in statement.lines
            ]

            return statement.id
