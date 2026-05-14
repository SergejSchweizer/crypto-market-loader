"""Architecture boundary tests using import-linter."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_import_linter_contracts_pass() -> None:
    """Validate import-linter contracts for package layering."""

    executable = Path(sys.executable).parent / "lint-imports"
    process = subprocess.run(
        [str(executable), "--config", ".importlinter"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert process.returncode == 0, process.stdout + process.stderr
