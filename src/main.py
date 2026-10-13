"""Main orchestration entry point for Marketplace Seller Mart pipeline."""
import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from src.ai_summary import AIProvider, generate_seller_summaries
from src.config import DB_PATH, PROJECT_ROOT, RAW_DIR
from src.db import init_db
from src.generate_data import main as generate_synthetic_data_main
from src.ingestion import ingest_all_files
from src.pipeline import run_metrics_pipeline, run_staging_pipeline

logger = logging.getLogger(__name__)

SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema.sql"


class PipelineError(Exception):
    """Raised when a required pipeline stage fails."""
    pass


def compute_iso_week(date_str: str) -> str:
    """Compute canonical ISO 8601 reporting week (YYYY-Www) for an order date."""
    from datetime import date
    d = date.fromisoformat(date_str)
    year, week, _ = d.isocalendar()
    return f"{year}-W{week:02d}"


def fail_stage(
    result: Dict[str, Any],
    stage_name: str,
    error_msg: str,
    raise_on_error: bool = False,
) -> None:
    """Record stage failure in result dict, log error, and optionally raise PipelineError."""
    result["status"] = "FAILED"
    result["error"] = error_msg
    result["stages"][stage_name] = {"status": "FAILED", "error": error_msg}
    logger.error("Stage %s failed: %s", stage_name, error_msg)
    print(f"  -> [{stage_name.upper()}] FAILED: {error_msg}")
    print("\nPipeline execution halted due to failure. [RESULT: FAILED]")
    if raise_on_error:
        raise PipelineError(f"Stage '{stage_name}' failed: {error_msg}")


def run_pipeline(
    db_path: Path = DB_PATH,
    raw_dir: Path = RAW_DIR,
    schema_path: Path = SCHEMA_PATH,
    generate_data: bool = False,
    overwrite_data: bool = False,
    ai_provider: Optional[AIProvider] = None,
    force_summaries: bool = False,
    raise_on_error: bool = False,
) -> Dict[str, Any]:
    """Execute the end-to-end pipeline in order with stage gating and status tracking.

    Order of execution:
    1. Schema initialization
    2. Data generation (optional/conditional)
    3. Raw CSV ingestion
    4. Staging validation and load
    5. Seller-weekly metrics computation
    6. AI summary generation (with deterministic fallback)

    Args:
        db_path: Path to the SQLite database.
        raw_dir: Path to directory containing raw CSV files.
        schema_path: Path to sql/schema.sql.
        generate_data: Whether to generate synthetic CSV data before running.
        overwrite_data: Whether to allow overwriting existing CSV files.
        ai_provider: Optional AIProvider instance for summary generation.
        force_summaries: Whether to force regenerating summaries even if metrics are unchanged.
        raise_on_error: Whether to raise PipelineError on failure instead of returning dict.

    Returns:
        Dictionary with overall pipeline status, stage statuses, and error details.
    """
    result = {
        "status": "SUCCESS",
        "stages": {},
        "error": None,
    }

    def _fail(stage_name: str, error_msg: str):
        fail_stage(result, stage_name, error_msg, raise_on_error=raise_on_error)

    print("=" * 60)
    print("Marketplace Seller Mart - Pipeline Execution")
    print("=" * 60)

    # 1. Schema Initialization
    print("[1/5] Initializing database schema...")
    try:
        init_db(db_path, schema_path)
        result["stages"]["schema_init"] = {"status": "SUCCESS"}
        print("  -> Schema initialized successfully.")
    except PipelineError:
        raise
    except Exception as exc:
        _fail("schema_init", f"Schema initialization error: {exc}")
        return result

    # 2. Data Generation (conditional)
    if generate_data:
        print("[DATA_GEN] Generating synthetic CSV data...")
        try:
            generate_synthetic_data_main(output_dir=raw_dir, overwrite=overwrite_data)
            result["stages"]["data_generation"] = {"status": "SUCCESS"}
            print("  -> Synthetic CSV data generated.")
        except PipelineError:
            raise
        except Exception as exc:
            _fail("data_generation", f"Data generation error: {exc}")
            return result
    else:
        result["stages"]["data_generation"] = {"status": "SKIPPED"}

    # 3. Raw Ingestion
    print("[2/5] Ingesting CSV files into raw layer...")
    try:
        ingest_results = ingest_all_files(db_path=db_path, raw_dir=raw_dir)
    except PipelineError:
        raise
    except Exception as exc:
        _fail("raw_ingestion", f"Ingestion error: {exc}")
        return result

    failed_files = [
        fname for fname, res in ingest_results.items()
        if res.get("status") == "FAILED"
    ]
    if failed_files:
        result["stages"]["raw_ingestion"] = {"status": "FAILED", "details": ingest_results}
        _fail("raw_ingestion", f"Ingestion failed for file(s): {', '.join(failed_files)}")
        return result

    result["stages"]["raw_ingestion"] = {"status": "SUCCESS", "details": ingest_results}
    print("  -> Raw ingestion completed successfully.")

    # 4. Staging Validation & Load
    print("[3/5] Validating and staging data...")
    try:
        run_staging_pipeline(db_path=db_path)
        result["stages"]["staging"] = {"status": "SUCCESS"}
        print("  -> Staging pipeline completed successfully.")
    except PipelineError:
        raise
    except Exception as exc:
        _fail("staging", f"Staging error: {exc}")
        return result

    # 5. Seller-Weekly Metrics
    print("[4/5] Calculating seller-weekly metrics...")
    try:
        metrics_count = run_metrics_pipeline(db_path=db_path)
        result["stages"]["metrics"] = {"status": "SUCCESS", "rows": metrics_count}
        print(f"  -> Metrics calculated successfully ({metrics_count} active seller-weeks).")
    except PipelineError:
        raise
    except Exception as exc:
        _fail("metrics", f"Metrics calculation error: {exc}")
        return result

    # 6. AI Summary Generation (with deterministic fallback)
    print("[5/5] Generating weekly performance summaries...")
    try:
        summaries_count = generate_seller_summaries(
            db_path=db_path,
            provider=ai_provider,
            force=force_summaries,
        )
        result["stages"]["ai_summaries"] = {"status": "SUCCESS", "updated": summaries_count}
        print(f"  -> Summaries generated/refreshed ({summaries_count} updated).")
    except PipelineError:
        raise
    except Exception as exc:
        _fail("ai_summaries", f"Summary generation error: {exc}")
        return result

    print("\nPipeline completed successfully! [RESULT: SUCCESS]")
    return result


def parse_args(argv=None):
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Marketplace Seller Mart pipeline orchestrator.")
    parser.add_argument("--db-path", type=Path, default=DB_PATH, help="Path to SQLite database.")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR, help="Directory containing raw CSVs.")
    parser.add_argument(
        "--generate-data",
        action="store_true",
        help="Generate synthetic CSV data before running pipeline.",
    )
    parser.add_argument(
        "--overwrite-data",
        action="store_true",
        help="Allow overwriting existing synthetic CSV files.",
    )
    parser.add_argument(
        "--force-summaries",
        action="store_true",
        help="Force regeneration of AI summaries even if metrics are unchanged.",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    """Main CLI entry point."""
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # If --generate-data is not explicitly specified, check if CSV files are present.
    # If no CSV files exist in raw_dir, automatically generate them.
    generate = args.generate_data
    if not generate:
        required_csvs = ["sellers.csv", "products.csv", "orders.csv", "order_items.csv", "returns.csv"]
        if not any((args.raw_dir / f).exists() for f in required_csvs):
            logger.info("Raw CSV files not found in %s; generating synthetic dataset.", args.raw_dir)
            generate = True

    result = run_pipeline(
        db_path=args.db_path,
        raw_dir=args.raw_dir,
        generate_data=generate,
        overwrite_data=args.overwrite_data,
        force_summaries=args.force_summaries,
    )

    return 0 if result["status"] == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
