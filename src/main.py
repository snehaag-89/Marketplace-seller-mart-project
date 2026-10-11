"""Main entry point for the pipeline."""
import logging
from src.config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    logging.info("Starting Marketplace Seller Mart pipeline...")
    logging.info(f"Database path: {DB_PATH}")
    # Pipeline execution will go here
    logging.info("Pipeline completed successfully.")

if __name__ == "__main__":
    main()
