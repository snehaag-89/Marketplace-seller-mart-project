"""Focused tests for Phase 4 AI summary generation and fallback."""
import pytest
import sqlite3
from pathlib import Path

from src.db import init_db, get_db_connection
from src.config import PROJECT_ROOT
from src.ai_summary import (
    AIProvider,
    AIProviderError,
    LocalOllamaProvider,
    MockAIProvider,
    compute_metrics_hash,
    format_prompt,
    generate_deterministic_summary,
    generate_seller_summaries,
    init_summary_tracking,
    is_valid_summary,
)

SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema.sql"


@pytest.fixture
def test_db_path(tmp_path):
    """Create a temporary initialized SQLite database."""
    db_path = tmp_path / "test_ai_summary.db"
    init_db(db_path, SCHEMA_PATH)
    return db_path


def seed_metrics_row(
    db_path: Path,
    seller_id: str = "S1",
    week: str = "2026-W36",
    sales: float = 1500.0,
    return_rate: float = 0.04,
    cancel_rate: float = 0.02,
    summary: str = None,
):
    """Helper to seed a row into stg_sellers and mart_seller_weekly_metrics."""
    with get_db_connection(db_path) as conn:
        conn.execute("INSERT OR IGNORE INTO stg_sellers VALUES (?, ?)", (seller_id, f"Seller {seller_id}"))
        conn.execute(
            """
            INSERT INTO mart_seller_weekly_metrics 
                (seller_id, reporting_week, sales_volume, order_return_rate, cancellation_rate, ai_summary)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(seller_id, reporting_week) DO UPDATE SET
                sales_volume = excluded.sales_volume,
                order_return_rate = excluded.order_return_rate,
                cancellation_rate = excluded.cancellation_rate,
                ai_summary = excluded.ai_summary
            """,
            (seller_id, week, sales, return_rate, cancel_rate, summary),
        )


def test_deterministic_fallback_normal_values():
    """Verify deterministic fallback format with standard business numbers."""
    metrics = {
        "seller_id": "S1",
        "reporting_week": "2026-W36",
        "sales_volume": 1250.50,
        "order_return_rate": 0.05,
        "cancellation_rate": 0.02,
    }
    summary = generate_deterministic_summary(metrics)

    assert "Seller S1" in summary
    assert "2026-W36" in summary
    assert "$1,250.50" in summary
    assert "5.0%" in summary
    assert "2.0%" in summary
    assert "return rate unavailable" not in summary
    assert "cancellation rate is unavailable" not in summary


def test_deterministic_fallback_null_rates():
    """Verify NULL return and cancellation rates are explicitly explained, never converted to 0%."""
    metrics = {
        "seller_id": "S2",
        "reporting_week": "2026-W36",
        "sales_volume": 0.0,
        "order_return_rate": None,
        "cancellation_rate": None,
    }
    summary = generate_deterministic_summary(metrics)

    assert "Seller S2" in summary
    assert "$0.00" in summary
    assert "return rate unavailable (no eligible orders)" in summary
    assert "cancellation rate is unavailable (no orders placed)" in summary
    # Must NOT claim 0.0% return rate or 0.0% cancellation rate
    assert "0.0% return" not in summary
    assert "0.0% cancellation" not in summary


def test_deterministic_fallback_zero_vs_elevated_rates():
    """Verify clean distinction between 0% fulfillment and elevated (>10%) concerns."""
    # Zero rates
    metrics_zero = {
        "seller_id": "S1",
        "reporting_week": "2026-W36",
        "sales_volume": 500.0,
        "order_return_rate": 0.0,
        "cancellation_rate": 0.0,
    }
    summary_zero = generate_deterministic_summary(metrics_zero)
    assert "zero returns on eligible orders" in summary_zero
    assert "no orders were cancelled" in summary_zero

    # Elevated rates
    metrics_high = {
        "seller_id": "S1",
        "reporting_week": "2026-W36",
        "sales_volume": 500.0,
        "order_return_rate": 0.18,
        "cancellation_rate": 0.12,
    }
    summary_high = generate_deterministic_summary(metrics_high)
    assert "elevated return rate of 18.0%" in summary_high
    assert "elevated cancellation rate of 12.0%" in summary_high


def test_format_prompt_grounding():
    """Verify the prompt explicitly instructs the model to preserve NULL rates and avoid hallucination."""
    metrics = {
        "seller_id": "S1",
        "reporting_week": "2026-W36",
        "sales_volume": 350.0,
        "order_return_rate": None,
        "cancellation_rate": 0.05,
    }
    prompt = format_prompt(metrics)

    assert "S1" in prompt
    assert "2026-W36" in prompt
    assert "$350.00" in prompt
    assert "5.0%" in prompt
    assert "Unavailable / Not applicable" in prompt
    assert "Do not invent causes, recommendations" in prompt


def test_successful_generation_mock_provider(test_db_path):
    """Verify successful generation using a mocked AI provider."""
    seed_metrics_row(test_db_path, seller_id="S1", week="2026-W36", sales=2000.0)

    ai_text = "Seller S1 demonstrated strong sales of $2,000.00 in Week 36 with healthy fulfillment metrics."
    mock = MockAIProvider(responder=ai_text)

    updated = generate_seller_summaries(test_db_path, provider=mock)
    assert updated == 1
    assert mock.call_count == 1

    with get_db_connection(test_db_path) as conn:
        row = conn.execute(
            "SELECT ai_summary FROM mart_seller_weekly_metrics WHERE seller_id = 'S1'"
        ).fetchone()
        assert row is not None
        assert row[0] == ai_text


def test_provider_unavailable_triggers_fallback(test_db_path):
    """Verify that an unavailable provider triggers deterministic fallback without breaking."""
    seed_metrics_row(test_db_path, seller_id="S1", week="2026-W36", sales=1200.0, return_rate=0.03, cancel_rate=0.01)

    mock = MockAIProvider(responder=AIProviderError("Local service connection refused"))

    updated = generate_seller_summaries(test_db_path, provider=mock)
    assert updated == 1

    with get_db_connection(test_db_path) as conn:
        row = conn.execute(
            "SELECT ai_summary FROM mart_seller_weekly_metrics WHERE seller_id = 'S1'"
        ).fetchone()
        assert row is not None
        assert "Seller S1 performance summary for 2026-W36:" in row[0]
        assert "$1,200.00" in row[0]
        assert "3.0%" in row[0]


def test_provider_exception_triggers_fallback(test_db_path):
    """Verify that an unexpected provider exception triggers fallback cleanly."""
    seed_metrics_row(test_db_path, seller_id="S1", week="2026-W36", sales=850.0)

    mock = MockAIProvider(responder=RuntimeError("Out of memory on GPU"))

    updated = generate_seller_summaries(test_db_path, provider=mock)
    assert updated == 1

    with get_db_connection(test_db_path) as conn:
        row = conn.execute(
            "SELECT ai_summary FROM mart_seller_weekly_metrics WHERE seller_id = 'S1'"
        ).fetchone()
        assert row is not None
        assert "Seller S1 performance summary for 2026-W36:" in row[0]
        assert "$850.00" in row[0]


def test_empty_or_invalid_provider_response_triggers_fallback(test_db_path):
    """Verify that empty, whitespace, or stub responses trigger deterministic fallback."""
    seed_metrics_row(test_db_path, seller_id="S1", week="2026-W36", sales=750.0)

    for invalid_resp in ["", "   ", "error: model timed out", "short"]:
        mock = MockAIProvider(responder=invalid_resp)
        generate_seller_summaries(test_db_path, provider=mock, force=True)

        with get_db_connection(test_db_path) as conn:
            row = conn.execute(
                "SELECT ai_summary FROM mart_seller_weekly_metrics WHERE seller_id = 'S1'"
            ).fetchone()
            assert row is not None
            # Must have fallen back to deterministic template
            assert "Seller S1 performance summary for 2026-W36:" in row[0]
            assert "$750.00" in row[0]


def test_summary_persistence(test_db_path):
    """Verify that generated summaries are durably written to SQLite database."""
    seed_metrics_row(test_db_path, seller_id="S1", week="2026-W36", sales=300.0)
    seed_metrics_row(test_db_path, seller_id="S2", week="2026-W36", sales=600.0)

    mock = MockAIProvider(responder=lambda m: f"Valid custom summary for {m['seller_id']} with sales ${m['sales_volume']:.2f}")
    updated = generate_seller_summaries(test_db_path, provider=mock)
    assert updated == 2

    # Query with a completely fresh connection to prove disk persistence
    with get_db_connection(test_db_path) as conn:
        rows = conn.execute(
            "SELECT seller_id, ai_summary FROM mart_seller_weekly_metrics ORDER BY seller_id"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][0] == "S1"
        assert rows[0][1] == "Valid custom summary for S1 with sales $300.00"
        assert rows[1][0] == "S2"
        assert rows[1][1] == "Valid custom summary for S2 with sales $600.00"


def test_unchanged_metrics_skips_unnecessary_regeneration(test_db_path):
    """Verify that if metrics have not changed, summary generation does not overwrite or call provider."""
    seed_metrics_row(test_db_path, seller_id="S1", week="2026-W36", sales=100.0)

    mock = MockAIProvider(responder="Initial AI Summary V1 for S1")

    # Run 1: generates summary
    count_1 = generate_seller_summaries(test_db_path, provider=mock)
    assert count_1 == 1
    assert mock.call_count == 1

    # Run 2: metrics are identical, provider should NOT be called
    count_2 = generate_seller_summaries(test_db_path, provider=mock)
    assert count_2 == 0
    assert mock.call_count == 1  # Still 1!

    with get_db_connection(test_db_path) as conn:
        summary = conn.execute(
            "SELECT ai_summary FROM mart_seller_weekly_metrics WHERE seller_id = 'S1'"
        ).fetchone()[0]
        assert summary == "Initial AI Summary V1 for S1"


def test_changed_metrics_causes_summary_refresh(test_db_path):
    """Verify that when underlying metrics change, the summary is refreshed."""
    seed_metrics_row(test_db_path, seller_id="S1", week="2026-W36", sales=100.0, return_rate=0.01)

    mock = MockAIProvider(
        responder=lambda m: f"Summary for {m['seller_id']}: Sales=${m['sales_volume']:.2f}, Return={m['order_return_rate']}"
    )

    # Run 1: initial generation
    generate_seller_summaries(test_db_path, provider=mock)
    assert mock.call_count == 1

    with get_db_connection(test_db_path) as conn:
        s1 = conn.execute("SELECT ai_summary FROM mart_seller_weekly_metrics WHERE seller_id = 'S1'").fetchone()[0]
        assert "Sales=$100.00" in s1

    # Change the metrics (e.g. late return arrived or sales updated)
    with get_db_connection(test_db_path) as conn:
        conn.execute(
            "UPDATE mart_seller_weekly_metrics SET sales_volume = 450.0, order_return_rate = 0.15 WHERE seller_id = 'S1'"
        )

    # Run 2: metrics changed -> summary must be refreshed
    updated = generate_seller_summaries(test_db_path, provider=mock)
    assert updated == 1
    assert mock.call_count == 2

    with get_db_connection(test_db_path) as conn:
        s2 = conn.execute("SELECT ai_summary FROM mart_seller_weekly_metrics WHERE seller_id = 'S1'").fetchone()[0]
        assert "Sales=$450.00" in s2
        assert "Return=0.15" in s2


def test_offline_operation_without_ai_service(test_db_path):
    """Verify that the pipeline operates fully offline when no AI service is present (default LocalOllamaProvider)."""
    seed_metrics_row(test_db_path, seller_id="S1", week="2026-W36", sales=999.0)

    # Passing no provider uses default LocalOllamaProvider pointing to an offline localhost port
    provider = LocalOllamaProvider(base_url="http://127.0.0.1:59999", timeout=0.1)

    updated = generate_seller_summaries(test_db_path, provider=provider)
    assert updated == 1

    with get_db_connection(test_db_path) as conn:
        summary = conn.execute(
            "SELECT ai_summary FROM mart_seller_weekly_metrics WHERE seller_id = 'S1'"
        ).fetchone()[0]
        assert "Seller S1 performance summary for 2026-W36:" in summary
        assert "$999.00" in summary


def test_empty_existing_summary_with_matching_tracking_hash_regenerated(test_db_path):
    """Regression test: An empty existing summary with a matching tracking hash gets regenerated."""
    seed_metrics_row(test_db_path, seller_id="S1", week="2026-W36", sales=500.0, return_rate=0.05, cancel_rate=0.01, summary="")
    h = compute_metrics_hash(500.0, 0.05, 0.01)
    with get_db_connection(test_db_path) as conn:
        init_summary_tracking(conn)
        conn.execute(
            "INSERT INTO mart_summary_tracking (seller_id, reporting_week, metrics_hash) VALUES ('S1', '2026-W36', ?)",
            (h,),
        )

    mock = MockAIProvider(responder="Regenerated valid AI summary for seller S1")
    updated = generate_seller_summaries(test_db_path, provider=mock, force=False)
    assert updated == 1
    assert mock.call_count == 1

    with get_db_connection(test_db_path) as conn:
        summary = conn.execute(
            "SELECT ai_summary FROM mart_seller_weekly_metrics WHERE seller_id = 'S1'"
        ).fetchone()[0]
        assert summary == "Regenerated valid AI summary for seller S1"


def test_whitespace_existing_summary_with_matching_tracking_hash_regenerated(test_db_path):
    """Regression test: A whitespace-only existing summary with a matching tracking hash gets regenerated."""
    seed_metrics_row(test_db_path, seller_id="S1", week="2026-W36", sales=600.0, return_rate=0.02, cancel_rate=0.0, summary="   \n\t  ")
    h = compute_metrics_hash(600.0, 0.02, 0.0)
    with get_db_connection(test_db_path) as conn:
        init_summary_tracking(conn)
        conn.execute(
            "INSERT INTO mart_summary_tracking (seller_id, reporting_week, metrics_hash) VALUES ('S1', '2026-W36', ?)",
            (h,),
        )

    mock = MockAIProvider(responder="Fresh regenerated summary for seller S1 fulfillment")
    updated = generate_seller_summaries(test_db_path, provider=mock, force=False)
    assert updated == 1
    assert mock.call_count == 1

    with get_db_connection(test_db_path) as conn:
        summary = conn.execute(
            "SELECT ai_summary FROM mart_seller_weekly_metrics WHERE seller_id = 'S1'"
        ).fetchone()[0]
        assert summary == "Fresh regenerated summary for seller S1 fulfillment"


def test_valid_existing_summary_with_unchanged_metrics_skipped(test_db_path):
    """Regression test: A valid existing summary with unchanged metrics is skipped and provider is not called."""
    valid_summary = "Seller S1 performance summary for 2026-W36: Generated $700.00 in sales volume."
    seed_metrics_row(test_db_path, seller_id="S1", week="2026-W36", sales=700.0, return_rate=0.01, cancel_rate=0.01, summary=valid_summary)
    h = compute_metrics_hash(700.0, 0.01, 0.01)
    with get_db_connection(test_db_path) as conn:
        init_summary_tracking(conn)
        conn.execute(
            "INSERT INTO mart_summary_tracking (seller_id, reporting_week, metrics_hash) VALUES ('S1', '2026-W36', ?)",
            (h,),
        )

    mock = MockAIProvider(responder="This should never be called")
    updated = generate_seller_summaries(test_db_path, provider=mock, force=False)
    assert updated == 0
    assert mock.call_count == 0

    with get_db_connection(test_db_path) as conn:
        summary = conn.execute(
            "SELECT ai_summary FROM mart_seller_weekly_metrics WHERE seller_id = 'S1'"
        ).fetchone()[0]
        assert summary == valid_summary

