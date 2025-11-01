from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Tuple


def _load_env_file(env_path: Path) -> bool:
    if not env_path.exists():
        return False

    loaded = False
    try:
        with env_path.open("r", encoding="utf-8") as env_file:
            for raw_line in env_file:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and value and key not in os.environ:
                    os.environ[key] = value
                    loaded = True
    except OSError:
        return False

    return loaded


def load_env_files(candidates: Iterable[Path]) -> None:
    for env_path in candidates:
        _load_env_file(env_path)


def get_project_root(reference: Path) -> Path:
    return reference.resolve().parent.parent.parent


def get_cemaden_credentials(reference_file: Path | None = None) -> Tuple[str, str]:
    if reference_file is None:
        reference_file = Path(__file__)

    project_root = get_project_root(reference_file)
    env_candidates = [
        project_root / ".env",
        project_root / "config" / ".env",
    ]

    load_env_files(env_candidates)

    username = os.getenv("CEMADEN_USERNAME")
    password = os.getenv("CEMADEN_PASSWORD")

    missing = [
        name
        for name, value in (
            ("CEMADEN_USERNAME", username),
            ("CEMADEN_PASSWORD", password),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(
            f"Missing environment variable(s): {', '.join(missing)}. "
            "Ensure they are set or defined in a .env file."
        )

    return username, password
