"""Database connection and transaction management."""
import sqlite3
from pathlib import Path
from typing import Generator
import contextlib

@contextlib.contextmanager
def get_db_connection(db_path: Path) -> Generator[sqlite3.Connection, None, None]:
    """Provide a transactional scope around a series of operations.
    Commits on success, rolls back on exception, and ensures closure.
    """
    conn = sqlite3.connect(db_path)
    # Enable foreign keys
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db(db_path: Path, schema_path: Path) -> None:
    """Initialize the database schema idempotently."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    # Use a direct connection instead of the transactional context manager,
    # because executescript() implicitly issues a COMMIT and breaks normal transaction boundaries.
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_script = f.read()
        conn.executescript(schema_script)
    finally:
        conn.close()
