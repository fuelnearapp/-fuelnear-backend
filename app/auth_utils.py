

import base64
import hashlib
import hmac
import os
import secrets
import string
from typing import Final


PBKDF2_ALGORITHM: Final[str] = "sha256"
PBKDF2_ITERATIONS: Final[int] = max(600_000, int(os.getenv("PASSWORD_PBKDF2_ITERATIONS", "600000")))
LEGACY_PBKDF2_ITERATIONS: Final[int] = 100_000
PASSWORD_MIN_LENGTH: Final[int] = max(1, int(os.getenv("PASSWORD_MIN_LENGTH", "8")))
PASSWORD_MAX_LENGTH: Final[int] = max(PASSWORD_MIN_LENGTH, int(os.getenv("PASSWORD_MAX_LENGTH", "128")))
SALT_BYTES: Final[int] = 16
REFERRAL_CODE_LENGTH: Final[int] = 8
REFERRAL_ALPHABET: Final[str] = string.ascii_uppercase + string.digits
ACCESS_TOKEN_BYTES: Final[int] = 32
REFRESH_TOKEN_BYTES: Final[int] = 48


def _b64encode(value: bytes) -> str:
    return base64.b64encode(value).decode("utf-8")


def _b64decode(value: str) -> bytes:
    return base64.b64decode(value.encode("utf-8"))


def validate_password_length(password: str, *, check_minimum: bool = True) -> None:
    if check_minimum and (not password or len(password) < PASSWORD_MIN_LENGTH):
        raise ValueError("Password is too short")
    if len(password or "") > PASSWORD_MAX_LENGTH:
        raise ValueError("Password is too long")


def hash_password(password: str) -> str:
    validate_password_length(password)

    salt = secrets.token_bytes(SALT_BYTES)
    derived_key = hashlib.pbkdf2_hmac(
        PBKDF2_ALGORITHM,
        password.encode("utf-8"),
        salt,
        PBKDF2_ITERATIONS,
    )

    return f"pbkdf2_{PBKDF2_ALGORITHM}${PBKDF2_ITERATIONS}${_b64encode(salt)}${_b64encode(derived_key)}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        validate_password_length(password, check_minimum=False)
    except ValueError:
        return False

    try:
        scheme, iterations_str, salt_b64, derived_key_b64 = password_hash.split("$")
    except ValueError:
        return False

    if scheme != f"pbkdf2_{PBKDF2_ALGORITHM}":
        return False

    try:
        iterations = int(iterations_str)
        salt = _b64decode(salt_b64)
        expected_key = _b64decode(derived_key_b64)
    except (ValueError, TypeError):
        return False

    candidate_key = hashlib.pbkdf2_hmac(
        PBKDF2_ALGORITHM,
        password.encode("utf-8"),
        salt,
        iterations,
    )

    return hmac.compare_digest(candidate_key, expected_key)


def password_hash_is_legacy(password_hash: str) -> bool:
    try:
        scheme, iterations_str, _salt_b64, _derived_key_b64 = password_hash.split("$")
        iterations = int(iterations_str)
    except (ValueError, TypeError):
        return False

    return scheme == f"pbkdf2_{PBKDF2_ALGORITHM}" and iterations < PBKDF2_ITERATIONS


def hash_token(token: str) -> str:
    if not token:
        raise ValueError("Token cannot be empty")

    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def generate_access_token() -> str:
    return secrets.token_urlsafe(ACCESS_TOKEN_BYTES)


def generate_refresh_token() -> str:
    return secrets.token_urlsafe(REFRESH_TOKEN_BYTES)


def generate_referral_code(length: int = REFERRAL_CODE_LENGTH) -> str:
    if length < 6:
        raise ValueError("Referral code length must be at least 6")

    return "".join(secrets.choice(REFERRAL_ALPHABET) for _ in range(length))
