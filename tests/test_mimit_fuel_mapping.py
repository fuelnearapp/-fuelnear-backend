from __future__ import annotations

from datetime import datetime
import unittest

import pandas as pd

from app.import_mimit import (
    NormalizedMimitPrice,
    normalize_fuel_type,
    prepare_prices_for_import,
    select_deterministic_price_rows,
)


REPORTED_AT = datetime(2026, 7, 28, 8, 0, 0)


def price_row(
    raw_fuel: str,
    *,
    mimit_id: int = 3473,
    price: float = 1.8,
    is_self_service: bool = True,
    reported_at: datetime = REPORTED_AT,
) -> NormalizedMimitPrice:
    fuel_type = normalize_fuel_type(raw_fuel)
    if fuel_type is None:
        raise AssertionError(f"Test fixture fuel is not mapped: {raw_fuel}")
    return NormalizedMimitPrice(
        mimit_id=mimit_id,
        raw_fuel=raw_fuel,
        fuel_type=fuel_type,
        price=price,
        is_self_service=is_self_service,
        reported_at=reported_at,
    )


class MimitFuelMappingTests(unittest.TestCase):
    def test_benzina_standard_is_standard(self) -> None:
        self.assertEqual(normalize_fuel_type("Benzina"), "benzina")

    def test_blue_super_is_petrol_premium(self) -> None:
        self.assertEqual(normalize_fuel_type("Blue Super"), "benzina_premium")

    def test_diesel_standard_is_standard(self) -> None:
        self.assertEqual(normalize_fuel_type("Gasolio"), "diesel")

    def test_blue_diesel_is_diesel_premium(self) -> None:
        self.assertEqual(normalize_fuel_type("Blue Diesel"), "diesel_premium")

    def test_hi_q_diesel_is_diesel_premium(self) -> None:
        self.assertEqual(normalize_fuel_type("Hi-Q Diesel"), "diesel_premium")

    def test_supreme_diesel_is_diesel_premium(self) -> None:
        self.assertEqual(normalize_fuel_type("Supreme Diesel"), "diesel_premium")

    def test_v_power_diesel_does_not_become_petrol(self) -> None:
        self.assertEqual(normalize_fuel_type("Diesel Shell V Power"), "diesel_premium")

    def test_documented_commercial_diesels_are_premium(self) -> None:
        names = (
            "DieselMax",
            "E-DIESEL",
            "GP DIESEL",
            "Gasolio Energy D",
        )
        for name in names:
            with self.subTest(name=name):
                self.assertEqual(normalize_fuel_type(name), "diesel_premium")

    def test_documented_seasonal_diesels_are_standard(self) -> None:
        names = (
            "Gasolio Alpino",
            "Gasolio Gelo",
            "Gasolio artico",
            "Gasolio Artico",
        )
        for name in names:
            with self.subTest(name=name):
                self.assertEqual(normalize_fuel_type(name), "diesel")

    def test_documented_names_accept_case_and_extra_spaces(self) -> None:
        self.assertEqual(normalize_fuel_type("  dieselMAX  "), "diesel_premium")
        self.assertEqual(normalize_fuel_type("GP   DIESEL"), "diesel_premium")
        self.assertEqual(normalize_fuel_type(" GASOLIO   ARTICO "), "diesel")

    def test_unconfirmed_names_remain_excluded(self) -> None:
        self.assertIsNone(normalize_fuel_type("Gasolio Ecoplus"))
        self.assertIsNone(normalize_fuel_type("Gasolio Artico Igloo"))

    def test_standard_and_premium_coexist_for_same_station(self) -> None:
        selected = select_deterministic_price_rows(
            [price_row("Benzina"), price_row("Blue Super", price=2.0)]
        )
        self.assertEqual(
            {item.fuel_type for item in selected},
            {"benzina", "benzina_premium"},
        )

    def test_premium_only_does_not_create_standard(self) -> None:
        selected = select_deterministic_price_rows([price_row("Blue Diesel")])
        self.assertEqual([item.fuel_type for item in selected], ["diesel_premium"])

    def test_newest_observation_wins_within_same_group(self) -> None:
        older = price_row("Gasolio", price=1.7, reported_at=datetime(2026, 7, 27, 8))
        newer = price_row("Diesel", price=1.8, reported_at=datetime(2026, 7, 28, 8))
        self.assertEqual(select_deterministic_price_rows([newer, older]), [newer])

    def test_equal_timestamp_prefers_canonical_name(self) -> None:
        canonical = price_row("Gasolio", price=1.8)
        synonym = price_row("Diesel", price=1.7)
        self.assertEqual(select_deterministic_price_rows([synonym, canonical]), [canonical])

    def test_reversing_input_order_produces_same_result(self) -> None:
        rows = [
            price_row("Blue Diesel", price=1.91),
            price_row("Supreme Diesel", price=1.89),
            price_row("Gasolio", price=1.72),
        ]
        self.assertEqual(
            select_deterministic_price_rows(rows),
            select_deterministic_price_rows(reversed(rows)),
        )

    def test_unknown_name_is_not_assigned_to_standard(self) -> None:
        self.assertIsNone(normalize_fuel_type("Nuovo carburante commerciale"))

    def test_hvo_gpl_and_metano_have_no_regression(self) -> None:
        self.assertEqual(normalize_fuel_type("HVOlution"), "hvo")
        self.assertEqual(normalize_fuel_type("GPL"), "gpl")
        self.assertEqual(normalize_fuel_type("Metano"), "metano")

    def test_real_audit_samples_are_split_into_standard_and_premium(self) -> None:
        samples = {
            "Benzina": "benzina",
            "Blue Super": "benzina_premium",
            "Gasolio": "diesel",
            "Blue Diesel": "diesel_premium",
            "Supreme Diesel": "diesel_premium",
            "Hi-Q Diesel": "diesel_premium",
        }
        self.assertEqual(
            {raw: normalize_fuel_type(raw) for raw in samples},
            samples,
        )

    def test_prepare_prices_deduplicates_and_reports_unknown_names(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "idImpianto": "3473",
                    "descCarburante": "Benzina",
                    "prezzo": "1.800",
                    "isSelf": "1",
                    "dtComu": "28/07/2026 08:00:00",
                },
                {
                    "idImpianto": "3473",
                    "descCarburante": "Super",
                    "prezzo": "1.790",
                    "isSelf": "1",
                    "dtComu": "28/07/2026 08:00:00",
                },
                {
                    "idImpianto": "3473",
                    "descCarburante": "Prodotto sconosciuto",
                    "prezzo": "1.900",
                    "isSelf": "1",
                    "dtComu": "28/07/2026 08:00:00",
                },
            ]
        )
        selected, diagnostics = prepare_prices_for_import(frame, {3473: 1})

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0].raw_fuel, "Benzina")
        self.assertEqual(diagnostics.duplicate_rows_removed, 1)
        self.assertEqual(diagnostics.collision_groups, 1)
        self.assertEqual(diagnostics.unknown_fuel_rows, 1)
        self.assertEqual(diagnostics.unknown_fuel_names["Prodotto sconosciuto"], 1)


if __name__ == "__main__":
    unittest.main()
