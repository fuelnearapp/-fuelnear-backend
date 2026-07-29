import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from app import main


class MimitStatusTests(unittest.TestCase):
    def make_run(
        self,
        run_id: int,
        status: str,
        *,
        error_message: str | None = None,
        started_offset_minutes: int = 0,
    ) -> dict:
        started_at = datetime.now(timezone.utc) + timedelta(
            minutes=started_offset_minutes
        )
        return {
            "id": run_id,
            "started_at": started_at,
            "completed_at": started_at + timedelta(minutes=1),
            "status": status,
            "stations_imported": 10 if status == "success" else None,
            "prices_imported": 20 if status == "success" else None,
            "stations_csv": 11 if status == "success" else None,
            "prices_csv": 21 if status == "success" else None,
            "source_file_timestamp": started_at if status == "success" else None,
            "error_message": error_message,
        }

    def get_status(
        self,
        *,
        last_run: dict | None,
        last_success: dict | None,
        last_failed: dict | None,
    ) -> dict:
        connection = MagicMock()
        cursor = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchone.side_effect = [last_run, last_success, last_failed]

        with (
            patch.object(main, "get_connection", return_value=connection),
            patch.object(main, "ensure_mimit_import_schema"),
        ):
            response = main.get_mimit_status()

        connection.close.assert_called_once_with()
        return response

    def test_success_run_has_null_last_error(self) -> None:
        success = self.make_run(1, "success")

        write_connection = MagicMock()
        write_cursor = MagicMock()
        write_connection.cursor.return_value.__enter__.return_value = write_cursor
        main.finish_mimit_import_run(
            write_connection,
            1,
            {
                "import": {
                    "stations_imported": 10,
                    "prices_imported": 20,
                    "stations_csv": 11,
                    "prices_csv": 21,
                }
            },
        )
        success_query = " ".join(write_cursor.execute.call_args.args[0].split())

        response = self.get_status(
            last_run=success,
            last_success=success,
            last_failed=None,
        )

        self.assertEqual(response["last_status"], "success")
        self.assertIsNone(response["last_error"])
        self.assertIsNotNone(response["last_run"]["completed_at"])
        self.assertIsNone(response["duration_seconds"])
        self.assertIn("status = 'success'", success_query)
        self.assertIn("completed_at = NOW()", success_query)
        self.assertIn("error_message = NULL", success_query)

    def test_failed_run_has_last_error(self) -> None:
        failure = self.make_run(1, "failed", error_message="download failed")

        write_connection = MagicMock()
        write_cursor = MagicMock()
        write_connection.cursor.return_value.__enter__.return_value = write_cursor
        main.fail_mimit_import_run(write_connection, 1, "download failed")
        failure_query = " ".join(write_cursor.execute.call_args.args[0].split())

        response = self.get_status(
            last_run=failure,
            last_success=None,
            last_failed=failure,
        )

        self.assertEqual(response["last_status"], "failed")
        self.assertEqual(response["last_error"], "Last MIMIT update failed")
        self.assertIsNotNone(response["last_run"]["completed_at"])
        self.assertIsNone(response["duration_seconds"])
        self.assertIn("status = 'failed'", failure_query)
        self.assertIn("completed_at = NOW()", failure_query)
        self.assertIn("error_message = %s", failure_query)
        self.assertEqual(write_cursor.execute.call_args.args[1][0], "download failed")

    def test_success_after_failure_clears_last_error(self) -> None:
        failure = self.make_run(1, "failed", error_message="download failed")
        success = self.make_run(2, "success", started_offset_minutes=2)

        response = self.get_status(
            last_run=success,
            last_success=success,
            last_failed=failure,
        )

        self.assertEqual(response["last_status"], "success")
        self.assertIsNone(response["last_error"])
        self.assertEqual(response["last_failed"]["id"], 1)

    def test_failure_after_success_updates_last_error(self) -> None:
        success = self.make_run(1, "success")
        failure = self.make_run(
            2,
            "failed",
            error_message="parser failed",
            started_offset_minutes=2,
        )

        response = self.get_status(
            last_run=failure,
            last_success=success,
            last_failed=failure,
        )

        self.assertEqual(response["last_status"], "failed")
        self.assertEqual(response["last_run"]["id"], 2)
        self.assertEqual(response["last_error"], "Last MIMIT update failed")

    def test_endpoint_status_fields_come_from_latest_run(self) -> None:
        older_failure = self.make_run(4, "failed", error_message="old failure")
        latest_success = self.make_run(5, "success", started_offset_minutes=2)

        response = self.get_status(
            last_run=latest_success,
            last_success=latest_success,
            last_failed=older_failure,
        )

        self.assertIsNone(response["run_id"])
        self.assertEqual(response["last_status"], "success")
        self.assertEqual(response["last_run"]["id"], 5)
        self.assertIsNone(response["last_error"])

    def test_endpoint_contract_is_unchanged(self) -> None:
        success = self.make_run(1, "success")

        response = self.get_status(
            last_run=success,
            last_success=success,
            last_failed=None,
        )

        self.assertEqual(
            set(response),
            {
                "status",
                "update_state",
                "update_in_progress",
                "run_id",
                "started_at",
                "duration_seconds",
                "stale",
                "stale_after_seconds",
                "last_status",
                "last_run",
                "last_success",
                "last_failed",
                "last_successful_update_at",
                "stations_imported",
                "prices_imported",
                "stations_csv",
                "prices_csv",
                "source_file_timestamp",
                "last_error",
            },
        )


if __name__ == "__main__":
    unittest.main()
