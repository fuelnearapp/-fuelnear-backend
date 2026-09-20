from __future__ import annotations

from dataclasses import dataclass

import jwt
from appstoreserverlibrary.models.JWSTransactionDecodedPayload import (
    JWSTransactionDecodedPayload,
)
from appstoreserverlibrary.signed_data_verifier import SignedDataVerifier

from app.apple_subscriptions import (
    AppleEconomicEvidence,
    AppleEconomicEvidenceStatus,
    normalize_apple_economic_evidence,
)


@dataclass(frozen=True, slots=True)
class VerifiedAppleTransactionPayload:
    payload: JWSTransactionDecodedPayload
    economic_evidence: AppleEconomicEvidence


def verify_apple_transaction_payload(
    verifier: SignedDataVerifier,
    signed_transaction: str,
) -> VerifiedAppleTransactionPayload:
    """Verify a transaction and inspect economic fields from that same JWS."""
    payload = verifier.verify_and_decode_signed_transaction(signed_transaction)

    try:
        raw_payload = jwt.decode(
            signed_transaction,
            options={
                "verify_signature": False,
                "verify_exp": False,
                "verify_aud": False,
            },
        )
        if not isinstance(raw_payload, dict):
            raise TypeError("Apple transaction payload must be an object")
        economic_evidence = normalize_apple_economic_evidence(
            raw_payload.get("price"),
            raw_payload.get("currency"),
        )
    except Exception:
        economic_evidence = AppleEconomicEvidence(
            AppleEconomicEvidenceStatus.INVALID,
            invalid_reason="raw_payload_unavailable",
        )

    return VerifiedAppleTransactionPayload(
        payload=payload,
        economic_evidence=economic_evidence,
    )
