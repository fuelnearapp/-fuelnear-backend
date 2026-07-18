from __future__ import annotations

from pathlib import Path
import unittest

from app.apple_config import (
    AppleSubscriptionsConfig,
    AppleSubscriptionsConfigurationError,
    load_apple_subscriptions_config,
    validate_apple_subscriptions_config,
)


class AppleSubscriptionsConfigTestCase(unittest.TestCase):
    def test_loads_complete_configuration(self):
        config = load_apple_subscriptions_config(
            {
                "APPLE_SUBSCRIPTIONS_BUNDLE_ID": " MB.FuelNear ",
                "APPLE_SUBSCRIPTIONS_ENVIRONMENT": " Production ",
                "APPLE_APP_ID": "123456789",
                "APPLE_ROOT_CERTIFICATES_PATH": " /app/certs/apple ",
                "APPLE_ENABLE_ONLINE_CHECKS": "true",
            }
        )

        self.assertEqual(config.bundle_id, "MB.FuelNear")
        self.assertEqual(config.environment, "production")
        self.assertEqual(config.app_id, 123456789)
        self.assertEqual(config.root_certificates_path, Path("/app/certs/apple"))
        self.assertTrue(config.enable_online_checks)

    def test_missing_required_values_are_reported_by_validation(self):
        config = load_apple_subscriptions_config({})

        with self.assertRaises(AppleSubscriptionsConfigurationError) as context:
            validate_apple_subscriptions_config(config)

        message = str(context.exception)
        self.assertIn("APPLE_SUBSCRIPTIONS_BUNDLE_ID is required", message)
        self.assertIn("APPLE_SUBSCRIPTIONS_ENVIRONMENT is required", message)
        self.assertIn("APPLE_ROOT_CERTIFICATES_PATH is required", message)

    def test_invalid_environment_is_rejected(self):
        config = load_apple_subscriptions_config(
            {
                "APPLE_SUBSCRIPTIONS_BUNDLE_ID": "MB.FuelNear",
                "APPLE_SUBSCRIPTIONS_ENVIRONMENT": "staging",
                "APPLE_ROOT_CERTIFICATES_PATH": "/app/certs/apple",
            }
        )

        with self.assertRaisesRegex(
            AppleSubscriptionsConfigurationError,
            "APPLE_SUBSCRIPTIONS_ENVIRONMENT must be sandbox or production",
        ):
            validate_apple_subscriptions_config(config)

    def test_valid_sandbox_configuration_does_not_require_app_id(self):
        config = AppleSubscriptionsConfig(
            bundle_id=" MB.FuelNear ",
            environment="Sandbox",
            app_id=None,
            root_certificates_path=Path("/app/certs/apple"),
            enable_online_checks=False,
        )

        validated = validate_apple_subscriptions_config(config)

        self.assertEqual(validated.bundle_id, "MB.FuelNear")
        self.assertEqual(validated.environment, "sandbox")
        self.assertFalse(validated.enable_online_checks)

    def test_production_configuration_requires_app_id(self):
        config = load_apple_subscriptions_config(
            {
                "APPLE_SUBSCRIPTIONS_BUNDLE_ID": "MB.FuelNear",
                "APPLE_SUBSCRIPTIONS_ENVIRONMENT": "production",
                "APPLE_ROOT_CERTIFICATES_PATH": "/app/certs/apple",
            }
        )

        with self.assertRaisesRegex(
            AppleSubscriptionsConfigurationError,
            "APPLE_APP_ID is required in production",
        ):
            validate_apple_subscriptions_config(config)

    def test_invalid_app_id_has_clear_error(self):
        with self.assertRaisesRegex(
            AppleSubscriptionsConfigurationError,
            "APPLE_APP_ID must be a positive integer",
        ):
            load_apple_subscriptions_config({"APPLE_APP_ID": "not-a-number"})

    def test_invalid_online_checks_value_has_clear_error(self):
        with self.assertRaisesRegex(
            AppleSubscriptionsConfigurationError,
            "APPLE_ENABLE_ONLINE_CHECKS must be true or false",
        ):
            load_apple_subscriptions_config(
                {"APPLE_ENABLE_ONLINE_CHECKS": "sometimes"}
            )


if __name__ == "__main__":
    unittest.main()
