"""Smoke tests for the repository foundation."""
import os
import sys
from pathlib import Path

# Ensure src is in the python path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import PROJECT_ROOT, DB_PATH

def test_project_structure():
    """Verify that important directories exist."""
    assert (PROJECT_ROOT / "src").exists(), "src directory is missing"
    assert (PROJECT_ROOT / "tests").exists(), "tests directory is missing"
    assert (PROJECT_ROOT / "data").exists(), "data directory is missing"
    assert (PROJECT_ROOT / "data" / "raw").exists(), "data/raw directory is missing"

def test_imports():
    """Verify that modules can be imported successfully."""
    import src.config
    import src.db
    import src.main
    assert True
