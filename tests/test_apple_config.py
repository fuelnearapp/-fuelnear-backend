from __future__ import annotations

from pathlib import Path
import unittest

from app.apple_config import (
    AppleSubscriptionsConfig,
    AppleSubscriptionsConfigurationError,
    get_apple_subscription_accepted_environments,
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
                "APPLE_SUBSCRIPTIONS_ACCEPTED_ENVIRONMENTS": " Production, Sandbox ",
            }
        )

        self.assertEqual(config.bundle_id, "MB.FuelNear")
        self.assertEqual(config.environment, "production")
        self.assertEqual(config.app_id, 123456789)
        self.assertEqual(config.root_certificates_path, Path("/app/certs/apple"))
        self.assertTrue(config.enable_online_checks)
        self.assertEqual(config.accepted_environments, ("production", "sandbox"))

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

    def test_invalid_accepted_environment_is_rejected(self):
        config = load_apple_subscriptions_config(
            {
                "APPLE_SUBSCRIPTIONS_BUNDLE_ID": "MB.FuelNear",
                "APPLE_SUBSCRIPTIONS_ENVIRONMENT": "production",
                "APPLE_SUBSCRIPTIONS_ACCEPTED_ENVIRONMENTS": "production,xcode",
                "APPLE_APP_ID": "123456789",
                "APPLE_ROOT_CERTIFICATES_PATH": "/app/certs/apple",
            }
        )

        with self.assertRaisesRegex(
            AppleSubscriptionsConfigurationError,
            "must contain only sandbox or production",
        ):
            validate_apple_subscriptions_config(config)

    def test_legacy_environment_remains_the_default_when_list_is_absent(self):
        config = AppleSubscriptionsConfig(
            bundle_id="MB.FuelNear",
            environment="sandbox",
            app_id=None,
            root_certificates_path=Path("/app/certs/apple"),
            enable_online_checks=True,
        )

        self.assertEqual(
            get_apple_subscription_accepted_environments(config),
            ("sandbox",),
        )

    def test_dual_environment_requires_production_app_id(self):
        config = AppleSubscriptionsConfig(
            bundle_id="MB.FuelNear",
            environment="sandbox",
            app_id=None,
            root_certificates_path=Path("/app/certs/apple"),
            enable_online_checks=True,
            accepted_environments=("sandbox", "production"),
        )

        with self.assertRaisesRegex(
            AppleSubscriptionsConfigurationError,
            "APPLE_APP_ID is required in production",
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
