from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import json
import unittest

import pandas as pd

from app.import_mimit import (
    build_mimit_import_summary,
    evaluate_mimit_import_warnings,
    get_unknown_fuel_details,
    log_mimit_import_summary,
    normalize_fuel_type,
    prepare_prices_for_import,
)


def price_frame(raw_names: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "idImpianto": str(index + 1),
                "descCarburante": raw_name,
                "prezzo": "1.800",
                "isSelf": "1",
                "dtComu": "28/07/2026 08:00:00",
            }
            for index, raw_name in enumerate(raw_names)
        ]
    )


def normal_metrics() -> dict[str, object]:
    return {
        "source_extraction_date": "2026-07-28",
        "stations_csv": 23923,
        "stations_imported": 23920,
        "stations_excluded": 3,
        "prices_csv": 93147,
        "prices_for_importable_stations": 92990,
        "candidate_rows": 92958,
        "final_rows": 92103,
        "duplicates_removed": 855,
        "final_duplicate_keys": 0,
        "unknown_fuel_rows": 32,
        "unknown_fuels": [
            {
                "raw_name": "Gasolio Artico Igloo",
                "normalized_name": "gasolio artico igloo",
                "count": 2,
                "example_station_ids": [4384],
                "known": True,
            },
            {
                "raw_name": "Gasolio Ecoplus",
                "normalized_name": "gasolio ecoplus",
                "count": 30,
                "example_station_ids": [13635, 30081, 35471, 37837, 38500],
                "known": True,
            },
        ],
        "fuel_type_counts": {"benzina": 34263, "diesel": 34155},
        "oldest_reported_at": "2016-01-15T11:11:00",
        "newest_reported_at": "2026-07-28T23:58:00",
    }


class MimitImportMonitoringTests(unittest.TestCase):
    def test_normal_import_builds_complete_summary(self) -> None:
        summary = build_mimit_import_summary(
            normal_metrics(),
            run_id=77,
            status="success",
            duration_seconds=12.3456,
        )

        self.assertEqual(summary["run_id"], 77)
        self.assertEqual(summary["status"], "success")
        self.assertEqual(summary["stations_excluded"], 3)
        self.assertEqual(summary["final_rows"], 92103)
        self.assertEqual(summary["duration_seconds"], 12.346)

    def test_unknown_names_are_aggregated_by_normalized_name(self) -> None:
        frame = price_frame(["Nuovo Fuel", " nuovo   fuel ", "Gasolio"])
        _, diagnostics = prepare_prices_for_import(frame, {1: 1, 2: 2, 3: 3})

        details = get_unknown_fuel_details(diagnostics)

        self.assertEqual(len(details), 1)
        self.assertEqual(details[0]["normalized_name"], "nuovo fuel")
        self.assertEqual(details[0]["count"], 2)

    def test_gasolio_ecoplus_is_a_known_unknown(self) -> None:
        _, diagnostics = prepare_prices_for_import(
            price_frame(["Gasolio Ecoplus"]),
            {1: 1},
        )

        details = get_unknown_fuel_details(diagnostics)

        self.assertTrue(details[0]["known"])

    def test_new_unknown_name_produces_warning_log(self) -> None:
        metrics = normal_metrics()
        metrics["unknown_fuels"] = [
            {
                "raw_name": "Nuovo Fuel",
                "normalized_name": "nuovo fuel",
                "count": 1,
                "example_station_ids": [1],
                "known": False,
            }
        ]
        summary = build_mimit_import_summary(
            metrics,
            run_id=1,
            status="success",
            duration_seconds=1,
        )

        with self.assertLogs("fuelnear.mimit", level="WARNING") as captured:
            log_mimit_import_summary(summary)

        self.assertTrue(any("new_unknown_fuel_names" in line for line in captured.output))

    def test_multiple_occurrences_have_one_aggregate(self) -> None:
        frame = price_frame(["Brand X", "Brand X", "BRAND X"])
        _, diagnostics = prepare_prices_for_import(frame, {1: 1, 2: 2, 3: 3})

        details = get_unknown_fuel_details(diagnostics)

        self.assertEqual(details[0]["count"], 3)

    def test_unknown_examples_are_limited_to_five_stations(self) -> None:
        frame = price_frame(["Brand X"] * 8)
        _, diagnostics = prepare_prices_for_import(frame, {index: index for index in range(1, 9)})

        details = get_unknown_fuel_details(diagnostics)

        self.assertEqual(details[0]["example_station_ids"], [1, 2, 3, 4, 5])

    def test_zero_rows_produces_warning(self) -> None:
        metrics = normal_metrics()
        metrics["prices_csv"] = 0
        metrics["final_rows"] = 0
        summary = build_mimit_import_summary(
            metrics,
            run_id=None,
            status="failed",
            duration_seconds=0,
        )

        codes = {warning["code"] for warning in summary["warnings"]}

        self.assertIn("zero_price_rows", codes)

    def test_stale_snapshot_produces_warning(self) -> None:
        metrics = normal_metrics()
        metrics["newest_reported_at"] = "2026-07-24T08:00:00"
        summary = build_mimit_import_summary(
            metrics,
            run_id=1,
            status="success",
            duration_seconds=1,
        )

        codes = {warning["code"] for warning in summary["warnings"]}

        self.assertIn("stale_reported_at", codes)

    def test_low_final_row_count_produces_warning(self) -> None:
        metrics = normal_metrics()
        metrics["final_rows"] = 70000
        summary = build_mimit_import_summary(
            metrics,
            run_id=1,
            status="success",
            duration_seconds=1,
        )

        codes = {warning["code"] for warning in summary["warnings"]}

        self.assertIn("final_rows_below_threshold", codes)

    def test_unknown_row_increase_produces_warning(self) -> None:
        metrics = normal_metrics()
        metrics["unknown_fuel_rows"] = 101
        summary = build_mimit_import_summary(
            metrics,
            run_id=1,
            status="success",
            duration_seconds=1,
        )

        codes = {warning["code"] for warning in summary["warnings"]}

        self.assertIn("unknown_fuel_rows_increased", codes)

    def test_zero_imported_stations_produces_warning(self) -> None:
        metrics = normal_metrics()
        metrics["stations_imported"] = 0
        summary = build_mimit_import_summary(
            metrics,
            run_id=1,
            status="failed",
            duration_seconds=1,
        )

        codes = {warning["code"] for warning in summary["warnings"]}

        self.assertIn("zero_imported_stations", codes)

    def test_final_duplicate_keys_produce_warning(self) -> None:
        metrics = normal_metrics()
        metrics["final_duplicate_keys"] = 1
        summary = build_mimit_import_summary(
            metrics,
            run_id=1,
            status="success",
            duration_seconds=1,
        )

        codes = {warning["code"] for warning in summary["warnings"]}

        self.assertIn("final_duplicate_keys", codes)

    def test_current_dataset_profile_has_no_warning(self) -> None:
        summary = build_mimit_import_summary(
            normal_metrics(),
            run_id=1,
            status="success",
            duration_seconds=1,
        )

        self.assertEqual(summary["warnings"], [])

    def test_monitoring_does_not_modify_selected_records(self) -> None:
        selected, diagnostics = prepare_prices_for_import(
            price_frame(["Gasolio", "Blue Diesel", "Gasolio Ecoplus"]),
            {1: 1, 2: 2, 3: 3},
        )
        original = deepcopy(selected)

        metrics = {
            "unknown_fuels": get_unknown_fuel_details(diagnostics),
            "final_rows": len(selected),
        }
        build_mimit_import_summary(
            metrics,
            run_id=1,
            status="success",
            duration_seconds=1,
        )

        self.assertEqual(selected, original)

    def test_existing_fuel_mapping_has_no_regression(self) -> None:
        expected = {
            "Benzina": "benzina",
            "Blue Super": "benzina_premium",
            "Gasolio": "diesel",
            "DieselMax": "diesel_premium",
            "HVO": "hvo",
            "GPL": "gpl",
            "Metano": "metano",
        }
        self.assertEqual(
            {name: normalize_fuel_type(name) for name in expected},
            expected,
        )

    def test_warning_evaluation_is_deterministic(self) -> None:
        summary = build_mimit_import_summary(
            normal_metrics(),
            run_id=1,
            status="success",
            duration_seconds=1,
        )
        first = evaluate_mimit_import_warnings(summary)
        second = evaluate_mimit_import_warnings(json.loads(json.dumps(summary)))

        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
