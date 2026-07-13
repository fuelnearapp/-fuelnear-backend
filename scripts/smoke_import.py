#!/usr/bin/env python3

from __future__ import annotations

import importlib
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

MODULES = (
    "app.main",
    "app.apns_client",
    "app.auth_utils",
    "app.import_mimit",
    "app.email_service",
    "app.db",
)


def main() -> int:
    for module_name in MODULES:
        try:
            importlib.import_module(module_name)
        except Exception as exc:
            print(f"[SMOKE] import_failed module={module_name} type={exc.__class__.__name__}", file=sys.stderr)
            return 1

    print("[SMOKE] import_ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
