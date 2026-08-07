from app.db import get_connection
from app.plus_entitlements import (
    backfill_plus_entitlement_components,
    ensure_plus_entitlement_schema,
)


def main() -> None:
    conn = get_connection()
    try:
        with conn:
            ensure_plus_entitlement_schema(conn)
            backfilled = backfill_plus_entitlement_components(conn)
        print(f"[PLUS] entitlement_components_backfilled={backfilled}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
