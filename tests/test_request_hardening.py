from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from pydantic import ValidationError

from app import main


class RequestHardeningTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(main.app)

    def request(self, path: str, ip: str = "203.0.113.10"):
        return SimpleNamespace(
            headers={"x-real-ip": ip},
            client=SimpleNamespace(host="127.0.0.1"),
            url=SimpleNamespace(path=path),
        )

    def test_social_auth_within_limit_and_over_limit(self):
        allowed = None
        limited = main.APIError(
            429,
            "RATE_LIMITED",
            "Troppi tentativi. Riprova più tardi.",
            headers={"Retry-After": "600"},
        )
        with patch.object(main, "check_auth_rate_limit", side_effect=[allowed, limited]):
            main.rate_limit_google_auth(self.request("/auth/google"))
            with self.assertRaises(main.APIError) as raised:
                main.rate_limit_google_auth(self.request("/auth/google"))

        self.assertEqual(raised.exception.status_code, 429)
        self.assertEqual(raised.exception.error_code, "RATE_LIMITED")
        self.assertEqual(raised.exception.headers["Retry-After"], "600")

    def test_google_and_apple_use_separate_provider_buckets(self):
        with patch.object(main, "check_auth_rate_limit") as limiter:
            request = self.request("/auth/google")
            main.rate_limit_google_auth(request)
            main.rate_limit_apple_auth(request)

        google_call, apple_call = limiter.call_args_list
        self.assertEqual(google_call.args[0], "/auth/google")
        self.assertEqual(apple_call.args[0], "/auth/apple")
        self.assertNotEqual(google_call.args[1], apple_call.args[1])

    def test_social_tokens_accept_normal_size_and_reject_oversize(self):
        self.assertEqual(main.GoogleAuthRequest(id_token="x" * 2048).id_token, "x" * 2048)
        self.assertEqual(
            main.AppleAuthRequest(identity_token="x" * 4096).identity_token,
            "x" * 4096,
        )
        with self.assertRaises(ValidationError):
            main.GoogleAuthRequest(id_token="x" * (main.MAX_SOCIAL_ID_TOKEN_LENGTH + 1))
        with self.assertRaises(ValidationError):
            main.AppleAuthRequest(identity_token="x" * (main.MAX_SOCIAL_ID_TOKEN_LENGTH + 1))

    def test_search_rejects_query_too_long_and_huge_limit(self):
        main.app.dependency_overrides[main.rate_limit_station_search] = lambda: None
        self.addCleanup(main.app.dependency_overrides.pop, main.rate_limit_station_search, None)

        too_long = self.client.get(
            "/stations/search",
            params={"q": "x" * (main.MAX_STATION_SEARCH_LENGTH + 1)},
        )
        huge_limit = self.client.get(
            "/stations/search",
            params={"q": "Anzio", "limit": 100000},
        )

        self.assertEqual(too_long.status_code, 422)
        self.assertEqual(huge_limit.status_code, 422)

    def test_nearby_rejects_invalid_radius_and_coordinates(self):
        main.app.dependency_overrides[main.rate_limit_station_nearby] = lambda: None
        self.addCleanup(main.app.dependency_overrides.pop, main.rate_limit_station_nearby, None)
        base = {"lat": 41.49, "lng": 12.61, "fuel_type": "benzina"}

        invalid_radius = self.client.get(
            "/stations/nearby",
            params={**base, "radius_km": 101},
        )
        invalid_lat = self.client.get(
            "/stations/nearby",
            params={**base, "lat": 91},
        )
        invalid_lng = self.client.get(
            "/stations/nearby",
            params={**base, "lng": 181},
        )

        self.assertEqual(invalid_radius.status_code, 422)
        self.assertEqual(invalid_lat.status_code, 422)
        self.assertEqual(invalid_lng.status_code, 422)

    def test_storekit_jws_too_large_is_rejected_before_processing(self):
        main.app.dependency_overrides[main.rate_limit_apple_subscription_ip] = lambda: None
        self.addCleanup(
            main.app.dependency_overrides.pop,
            main.rate_limit_apple_subscription_ip,
            None,
        )
        with patch.object(main.apple_purchase_processor, "process_apple_transaction") as processor:
            response = self.client.post(
                "/user/subscription/apple/verify",
                json={"signed_transaction": "x" * (main.MAX_STOREKIT_JWS_LENGTH + 1)},
                headers={"Authorization": "Bearer token"},
            )

        self.assertEqual(response.status_code, 400)
        processor.assert_not_called()

    def test_device_token_format_and_size_are_validated(self):
        valid = "ab" * 32
        self.assertEqual(
            main.DeviceTokenRequest(device_token=valid, platform="ios").device_token,
            valid,
        )
        for invalid in ("not-a-token", "a" * (main.MAX_APNS_DEVICE_TOKEN_LENGTH + 1), "a" * 33):
            with self.subTest(invalid_length=len(invalid)):
                with self.assertRaises(ValidationError):
                    main.DeviceTokenRequest(device_token=invalid, platform="ios")

    def test_location_validation_rejects_invalid_values(self):
        for values in (
            {"lat": 90.1, "lng": 0},
            {"lat": 0, "lng": -180.1},
            {"lat": 0, "lng": 0, "accuracy": 0},
            {"lat": 0, "lng": 0, "source": "x" * 101},
        ):
            with self.subTest(values=values):
                with self.assertRaises(ValidationError):
                    main.UserLocationRequest(**values)

    def test_rate_limit_buckets_are_separate_between_users(self):
        with patch.object(main, "check_auth_rate_limit") as limiter:
            main.enforce_owner_rate_limit(
                "/user/location", "user", 1, limit=10, window_seconds=60
            )
            main.enforce_owner_rate_limit(
                "/user/location", "user", 2, limit=10, window_seconds=60
            )

        first, second = limiter.call_args_list
        self.assertNotEqual(first.args[1], second.args[1])

    def test_community_rate_limit_prevents_database_side_effect(self):
        limited = main.APIError(429, "RATE_LIMITED", "Troppi tentativi. Riprova più tardi.")
        with (
            patch.object(main, "get_current_user_from_token", return_value={"id": 7}),
            patch.object(main, "enforce_owner_rate_limit", side_effect=limited),
            patch.object(main, "get_connection") as get_connection,
        ):
            with self.assertRaises(main.APIError) as raised:
                main.submit_station_community_price(
                    42,
                    main.CommunityPriceReportRequest(
                        fuel_type="benzina",
                        price=1.8,
                        is_self_service=True,
                    ),
                    "Bearer access",
                )

        self.assertEqual(raised.exception.status_code, 429)
        get_connection.assert_not_called()

    def test_apple_verify_rate_limit_prevents_crypto_or_db_processing(self):
        limited = main.APIError(429, "RATE_LIMITED", "Troppi tentativi. Riprova più tardi.")
        with (
            patch.object(main, "get_current_user_from_token", return_value={"id": 7}),
            patch.object(main, "enforce_owner_rate_limit", side_effect=limited),
            patch.object(main.apple_jws_verifier, "verify_apple_signed_transaction") as verifier,
            patch.object(main.apple_purchase_processor, "process_apple_transaction") as processor,
        ):
            with self.assertRaises(main.APIError) as raised:
                main.verify_current_user_apple_subscription(
                    main.AppleSubscriptionVerifyRequest(signed_transaction="signed-jws"),
                    "Bearer access",
                )

        self.assertEqual(raised.exception.status_code, 429)
        verifier.assert_not_called()
        processor.assert_not_called()

    def test_apple_notification_normal_payload_is_not_blocked(self):
        verified = SimpleNamespace(
            notification_uuid="notification-1",
            notification_type="TEST",
            subtype=None,
            environment="Sandbox",
        )
        processed = SimpleNamespace(handled=True, action="test")
        with (
            patch.object(
                main.apple_notification_verifier,
                "verify_app_store_notification",
                return_value=verified,
            ),
            patch.object(
                main.apple_notification_processor,
                "process_app_store_notification",
                return_value=processed,
            ),
        ):
            response = self.client.post(
                "/apple/notifications",
                json={"signedPayload": "signed-notification"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})


if __name__ == "__main__":
    unittest.main()
