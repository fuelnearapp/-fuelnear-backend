from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from collections.abc import Mapping
import os


APPLE_SUBSCRIPTIONS_ENVIRONMENTS = frozenset({"sandbox", "production"})


class AppleSubscriptionsConfigurationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class AppleSubscriptionsConfig:
    bundle_id: str
    environment: str
    app_id: int | None
    root_certificates_path: Path | None
    enable_online_checks: bool
    accepted_environments: tuple[str, ...] = ()


def _optional_text(environ: Mapping[str, str], name: str) -> str | None:
    value = environ.get(name)
    if value is None:
        return None
    return value.strip() or None


def _parse_optional_app_id(raw_value: str | None) -> int | None:
    if raw_value is None:
        return None
    try:
        return int(raw_value)
    except ValueError as exc:
        raise AppleSubscriptionsConfigurationError(
            "APPLE_APP_ID must be a positive integer"
        ) from exc


def _parse_boolean(raw_value: str | None, *, default: bool) -> bool:
    if raw_value is None:
        return default

    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise AppleSubscriptionsConfigurationError(
        "APPLE_ENABLE_ONLINE_CHECKS must be true or false"
    )


def _parse_accepted_environments(raw_value: str | None) -> tuple[str, ...]:
    if raw_value is None:
        return ()

    values: list[str] = []
    for item in raw_value.split(","):
        normalized = item.strip().lower()
        if normalized and normalized not in values:
            values.append(normalized)
    return tuple(values)


def load_apple_subscriptions_config(
    environ: Mapping[str, str] | None = None,
) -> AppleSubscriptionsConfig:
    source = os.environ if environ is None else environ
    bundle_id = _optional_text(source, "APPLE_SUBSCRIPTIONS_BUNDLE_ID") or ""
    environment = (_optional_text(source, "APPLE_SUBSCRIPTIONS_ENVIRONMENT") or "").lower()
    accepted_environments = _parse_accepted_environments(
        _optional_text(source, "APPLE_SUBSCRIPTIONS_ACCEPTED_ENVIRONMENTS")
    )
    app_id = _parse_optional_app_id(_optional_text(source, "APPLE_APP_ID"))
    root_certificates_path_value = _optional_text(
        source,
        "APPLE_ROOT_CERTIFICATES_PATH",
    )

    return AppleSubscriptionsConfig(
        bundle_id=bundle_id,
        environment=environment,
        app_id=app_id,
        root_certificates_path=(
            Path(root_certificates_path_value)
            if root_certificates_path_value is not None
            else None
        ),
        enable_online_checks=_parse_boolean(
            _optional_text(source, "APPLE_ENABLE_ONLINE_CHECKS"),
            default=True,
        ),
        accepted_environments=accepted_environments,
    )


def validate_apple_subscriptions_config(
    config: AppleSubscriptionsConfig,
) -> AppleSubscriptionsConfig:
    if not isinstance(config, AppleSubscriptionsConfig):
        raise AppleSubscriptionsConfigurationError(
            "config must be an AppleSubscriptionsConfig"
        )

    errors: list[str] = []
    bundle_id = config.bundle_id.strip() if isinstance(config.bundle_id, str) else ""
    environment = config.environment.strip().lower() if isinstance(config.environment, str) else ""
    accepted_environments = tuple(
        item.strip().lower()
        for item in config.accepted_environments
        if isinstance(item, str) and item.strip()
    )

    if not bundle_id:
        errors.append("APPLE_SUBSCRIPTIONS_BUNDLE_ID is required")
    if not environment:
        errors.append("APPLE_SUBSCRIPTIONS_ENVIRONMENT is required")
    elif environment not in APPLE_SUBSCRIPTIONS_ENVIRONMENTS:
        errors.append(
            "APPLE_SUBSCRIPTIONS_ENVIRONMENT must be sandbox or production"
        )
    unsupported_environments = sorted(
        set(accepted_environments) - APPLE_SUBSCRIPTIONS_ENVIRONMENTS
    )
    if unsupported_environments:
        errors.append(
            "APPLE_SUBSCRIPTIONS_ACCEPTED_ENVIRONMENTS must contain only sandbox or production"
        )
    if config.root_certificates_path is None:
        errors.append("APPLE_ROOT_CERTIFICATES_PATH is required")
    elif not isinstance(config.root_certificates_path, Path):
        errors.append("APPLE_ROOT_CERTIFICATES_PATH must be a filesystem path")
    if config.app_id is not None and (
        isinstance(config.app_id, bool)
        or not isinstance(config.app_id, int)
        or config.app_id <= 0
    ):
        errors.append("APPLE_APP_ID must be a positive integer")
    effective_environments = accepted_environments or ((environment,) if environment else ())
    if "production" in effective_environments and config.app_id is None:
        errors.append("APPLE_APP_ID is required in production")
    if not isinstance(config.enable_online_checks, bool):
        errors.append("APPLE_ENABLE_ONLINE_CHECKS must be true or false")

    if errors:
        raise AppleSubscriptionsConfigurationError("; ".join(errors))

    return replace(
        config,
        bundle_id=bundle_id,
        environment=environment,
        accepted_environments=accepted_environments,
    )


def get_apple_subscription_accepted_environments(
    config: AppleSubscriptionsConfig,
) -> tuple[str, ...]:
    validated = validate_apple_subscriptions_config(config)
    return validated.accepted_environments or (validated.environment,)
