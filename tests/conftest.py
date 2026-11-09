"""
Pytest configuration and fixture setup for AtmoSync.
Ensures the project root directory is always present on sys.path.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Add project root directory to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

