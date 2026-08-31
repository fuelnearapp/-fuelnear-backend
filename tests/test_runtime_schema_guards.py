from __future__ import annotations

import ast
from pathlib import Path
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPOSITORY_ROOT / "app"
DDL_MARKERS = (
    "CREATE TABLE",
    "CREATE INDEX",
    "CREATE UNIQUE INDEX",
    "CREATE OR REPLACE FUNCTION",
    "CREATE TRIGGER",
    "ALTER TABLE",
    "DROP TABLE",
    "DROP INDEX",
    "DROP TRIGGER",
    "TRUNCATE TABLE",
    "DO $$",
)
ALLOWED_SCHEMA_CALLERS = {
    "ensure_auth_provider_schema",
    "ensure_auth_schema",
    "ensure_core_schema",
    "on_startup",
}


def called_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


class RuntimeSchemaGuardTests(unittest.TestCase):
    def test_ddl_exists_only_inside_schema_initializers(self) -> None:
        violations: list[str] = []
        for path in APP_ROOT.glob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for function in (
                node
                for node in ast.walk(tree)
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            ):
                for literal in (
                    node.value
                    for node in ast.walk(function)
                    if isinstance(node, ast.Constant) and isinstance(node.value, str)
                ):
                    normalized = literal.upper()
                    if any(marker in normalized for marker in DDL_MARKERS):
                        if not (
                            function.name.startswith("ensure_")
                            and function.name.endswith("_schema")
                        ):
                            violations.append(f"{path.name}:{function.name}")
                            break

        self.assertEqual(violations, [])

    def test_schema_initializers_are_not_called_from_requests_or_jobs(self) -> None:
        violations: list[str] = []
        for path in APP_ROOT.glob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for function in (
                node
                for node in ast.walk(tree)
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            ):
                for call in (node for node in ast.walk(function) if isinstance(node, ast.Call)):
                    name = called_name(call)
                    if name and name.startswith("ensure_") and name.endswith("_schema"):
                        if function.name not in ALLOWED_SCHEMA_CALLERS:
                            violations.append(f"{path.name}:{function.name}->{name}")

        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
