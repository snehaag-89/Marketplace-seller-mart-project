"""Module for generating and persisting grounded seller-weekly summaries with AI and deterministic fallback."""
import json
import logging
import os
import sqlite3
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union

from src.config import DB_PATH
from src.db import get_db_connection
from src.hash_utils import compute_row_hash

logger = logging.getLogger(__name__)


class AIProviderError(Exception):
    """Exception raised when an AI provider fails or is unreachable."""
    pass


class AIProvider(ABC):
    """Abstract interface for AI summary providers."""

    @abstractmethod
    def generate_summary(self, metrics: Dict[str, Any]) -> Optional[str]:
        """Generate a weekly summary string from structured metrics.

        Args:
            metrics: Dictionary containing seller_id, reporting_week,
                sales_volume, order_return_rate, cancellation_rate.

        Returns:
            The generated summary text, or None if generation failed.
        """
        pass


class LocalOllamaProvider(AIProvider):
    """Local, free model provider using Ollama HTTP API (e.g. llama3.2, phi3)."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        timeout: float = 2.0,
    ):
        if base_url is None:
            base_url = (
                os.environ.get("OLLAMA_BASE_URL")
                or os.environ.get("AI_PROVIDER_URL")
                or "http://localhost:11434"
            )
        base_url = base_url.rstrip("/")
        if base_url.endswith("/api/generate"):
            base_url = base_url[:-len("/api/generate")]
        self.base_url = base_url

        if model is None:
            model = os.environ.get("OLLAMA_MODEL", "llama3.2")
        self.model = model
        self.timeout = timeout
        self._service_unavailable = False

    def generate_summary(self, metrics: Dict[str, Any]) -> Optional[str]:
        """Call local Ollama endpoint to generate summary."""
        if self._service_unavailable:
            raise AIProviderError(f"Local AI service at {self.base_url} is unavailable.")

        prompt = format_prompt(metrics)
        payload = json.dumps({
            "model": self.model,
            "prompt": prompt,
            "stream": False,
        }).encode("utf-8")

        req = urllib.request.Request(
            f"{self.base_url}/api/generate",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data.get("response", "")
        except Exception as exc:
            self._service_unavailable = True
            raise AIProviderError(f"Local AI service error at {self.base_url}: {exc}") from exc


class MockAIProvider(AIProvider):
    """Mock AI provider for deterministic testing without network dependencies."""

    def __init__(
        self,
        responder: Optional[Union[str, Exception, Callable[[Dict[str, Any]], Optional[str]]]] = None,
    ):
        self.responder = responder
        self.call_count = 0
        self.last_metrics: Optional[Dict[str, Any]] = None

    def generate_summary(self, metrics: Dict[str, Any]) -> Optional[str]:
        self.call_count += 1
        self.last_metrics = metrics

        if isinstance(self.responder, Exception):
            raise self.responder
        if callable(self.responder):
            return self.responder(metrics)
        if isinstance(self.responder, str):
            return self.responder
        return None


def format_prompt(metrics: Dict[str, Any]) -> str:
    """Format structured metrics into a prompt with strict grounding constraints."""
    seller_id = metrics.get("seller_id", "Unknown")
    reporting_week = metrics.get("reporting_week", "Unknown")
    sales_volume = metrics.get("sales_volume")
    return_rate = metrics.get("order_return_rate")
    cancel_rate = metrics.get("cancellation_rate")

    sales_str = f"${sales_volume:,.2f}" if sales_volume is not None else "Unavailable"
    return_str = (
        f"{return_rate * 100:.1f}%"
        if return_rate is not None
        else "Unavailable / Not applicable (no eligible shipped/delivered orders)"
    )
    cancel_str = (
        f"{cancel_rate * 100:.1f}%"
        if cancel_rate is not None
        else "Unavailable / Not applicable (no orders placed)"
    )

    return (
        f"You are an analytics assistant generating a concise weekly summary for marketplace seller {seller_id} "
        f"for reporting week {reporting_week}.\n\n"
        f"Data:\n"
        f"- Sales Volume: {sales_str}\n"
        f"- Order Return Rate: {return_str}\n"
        f"- Order Cancellation Rate: {cancel_str}\n\n"
        f"Instructions:\n"
        f"1. State performance concisely (2-3 sentences) based strictly on the provided data.\n"
        f"2. If return or cancellation rates are unavailable/not applicable, state so explicitly. Do not assume or state 0%.\n"
        f"3. Note elevated rates (>10%) or perfect fulfillment (0%) when applicable.\n"
        f"4. Do not invent causes, recommendations, seller facts, or benchmarks outside this data.\n\n"
        f"Summary:"
    )


def generate_deterministic_summary(metrics: Dict[str, Any]) -> str:
    """Generate a deterministic, rule-based summary grounded strictly in calculated metrics."""
    seller_id = metrics.get("seller_id", "Unknown")
    reporting_week = metrics.get("reporting_week", "Unknown")
    sales_volume = metrics.get("sales_volume")
    return_rate = metrics.get("order_return_rate")
    cancel_rate = metrics.get("cancellation_rate")

    # 1. Sales volume description
    if sales_volume is not None:
        sales_str = f"${sales_volume:,.2f}"
    else:
        sales_str = "unavailable"

    # 2. Return rate description (distinguishing 0.0 from None)
    if return_rate is not None:
        return_pct = f"{return_rate * 100:.1f}%"
        if return_rate == 0.0:
            return_clause = "with zero returns on eligible orders"
        elif return_rate > 0.10:
            return_clause = f"with an elevated return rate of {return_pct}"
        else:
            return_clause = f"with a return rate of {return_pct}"
    else:
        return_clause = "with return rate unavailable (no eligible orders)"

    # 3. Cancellation rate description (distinguishing 0.0 from None)
    if cancel_rate is not None:
        cancel_pct = f"{cancel_rate * 100:.1f}%"
        if cancel_rate == 0.0:
            cancel_clause = "no orders were cancelled"
        elif cancel_rate > 0.10:
            cancel_clause = f"an elevated cancellation rate of {cancel_pct} was observed"
        else:
            cancel_clause = f"a cancellation rate of {cancel_pct} was recorded"
    else:
        cancel_clause = "cancellation rate is unavailable (no orders placed)"

    return (
        f"Seller {seller_id} performance summary for {reporting_week}: "
        f"Generated {sales_str} in eligible sales volume {return_clause}. "
        f"For order fulfillment, {cancel_clause}."
    )


def is_valid_summary(summary: Optional[str]) -> bool:
    """Validate that a summary response is a non-empty, substantive string."""
    if not summary or not isinstance(summary, str):
        return False
    stripped = summary.strip()
    if len(stripped) < 20:
        return False
    if stripped.lower().startswith("error:"):
        return False
    return True


def compute_metrics_hash(
    sales_volume: Optional[float],
    return_rate: Optional[float],
    cancel_rate: Optional[float],
) -> str:
    """Compute a canonical SHA-256 hash of business metrics for change detection."""
    record = {
        "sales_volume": sales_volume,
        "order_return_rate": return_rate,
        "cancellation_rate": cancel_rate,
    }
    return compute_row_hash(record)


def init_summary_tracking(conn: sqlite3.Connection) -> None:
    """Ensure the summary tracking table exists for incremental change detection."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mart_summary_tracking (
            seller_id TEXT NOT NULL,
            reporting_week TEXT NOT NULL,
            metrics_hash TEXT NOT NULL,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (seller_id, reporting_week)
        )
        """
    )


def generate_seller_summaries(
    db_path: Path = DB_PATH,
    provider: Optional[AIProvider] = None,
    force: bool = False,
) -> int:
    """Generate and persist weekly performance summaries into mart_seller_weekly_metrics.

    Args:
        db_path: Path to the SQLite database.
        provider: AIProvider instance. If None, defaults to LocalOllamaProvider
            with fallback to deterministic generator.
        force: If True, regenerates summaries even if metrics are unchanged.

    Returns:
        Number of summaries generated or refreshed.
    """
    if provider is None:
        provider = LocalOllamaProvider()

    with get_db_connection(db_path) as conn:
        init_summary_tracking(conn)

        # Prune stale tracking rows for weeks that no longer exist in the mart
        conn.execute(
            """
            DELETE FROM mart_summary_tracking
            WHERE (seller_id, reporting_week) NOT IN (
                SELECT seller_id, reporting_week FROM mart_seller_weekly_metrics
            )
            """
        )

        rows = conn.execute(
            """
            SELECT 
                m.seller_id,
                m.reporting_week,
                m.sales_volume,
                m.order_return_rate,
                m.cancellation_rate,
                m.ai_summary,
                t.metrics_hash
            FROM mart_seller_weekly_metrics m
            LEFT JOIN mart_summary_tracking t
                ON m.seller_id = t.seller_id AND m.reporting_week = t.reporting_week
            ORDER BY m.seller_id, m.reporting_week
            """
        ).fetchall()

        updated_count = 0

        for seller_id, week, sales, ret_rate, cancel_rate, existing_summary, stored_hash in rows:
            current_hash = compute_metrics_hash(sales, ret_rate, cancel_rate)

            # Skip regeneration only if not forced, metrics have not changed, and existing summary is valid
            if not force and is_valid_summary(existing_summary) and stored_hash == current_hash:
                continue

            payload = {
                "seller_id": seller_id,
                "reporting_week": week,
                "sales_volume": sales,
                "order_return_rate": ret_rate,
                "cancellation_rate": cancel_rate,
            }

            summary_text = None
            if provider is not None:
                try:
                    raw_resp = provider.generate_summary(payload)
                    if is_valid_summary(raw_resp):
                        summary_text = raw_resp.strip()
                    else:
                        logger.warning(
                            "Invalid or empty summary response for seller %s week %s; using deterministic fallback.",
                            seller_id,
                            week,
                        )
                        summary_text = generate_deterministic_summary(payload)
                except Exception as exc:
                    logger.warning(
                        "AI provider error for seller %s week %s: %s; using deterministic fallback.",
                        seller_id,
                        week,
                        exc,
                    )
                    summary_text = generate_deterministic_summary(payload)
            else:
                summary_text = generate_deterministic_summary(payload)

            conn.execute(
                """
                UPDATE mart_seller_weekly_metrics
                SET ai_summary = ?
                WHERE seller_id = ? AND reporting_week = ?
                """,
                (summary_text, seller_id, week),
            )

            conn.execute(
                """
                INSERT INTO mart_summary_tracking (seller_id, reporting_week, metrics_hash)
                VALUES (?, ?, ?)
                ON CONFLICT(seller_id, reporting_week) DO UPDATE SET
                    metrics_hash = excluded.metrics_hash,
                    generated_at = CURRENT_TIMESTAMP
                """,
                (seller_id, week, current_hash),
            )

            updated_count += 1

        return updated_count
