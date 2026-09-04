from __future__ import annotations

import shutil
import tempfile
import zipapp
from pathlib import Path


root = Path(__file__).resolve().parent
target = root / "cairn.pyz"
with tempfile.TemporaryDirectory() as temporary:
    stage = Path(temporary)
    shutil.copytree(
        root / "src" / "cairn",
        stage / "cairn",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    (stage / "__main__.py").write_text(
        "from cairn.cli import main\nraise SystemExit(main())\n",
        encoding="utf-8",
    )
    zipapp.create_archive(stage, target, interpreter="/usr/bin/env python3", compressed=True)
target.chmod(0o755)
print(target)
