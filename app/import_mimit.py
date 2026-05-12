import csv
import os
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen
import ssl
import shutil
import certifi

import pandas as pd
import psycopg2


DB_NAME = os.getenv("DB_NAME", "fuelnear")
DB_USER = os.getenv("DB_USER", "matteo")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DATABASE_URL = os.getenv("DATABASE_URL")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
ANAGRAFICA_PATH = DATA_DIR / "anagrafica_impianti_attivi.csv"
PREZZI_PATH = DATA_DIR / "prezzo_alle_8.csv"

ANAGRAFICA_URL = os.getenv(
    "ANAGRAFICA_URL",
    "https://www.mimit.gov.it/images/exportCSV/anagrafica_impianti_attivi.csv",
)
PREZZI_URL = os.getenv(
    "PREZZI_URL",
    "https://www.mimit.gov.it/images/exportCSV/prezzo_alle_8.csv",
)


def get_connection():
    if DATABASE_URL:
        parsed = urlparse(DATABASE_URL)
        return psycopg2.connect(
            dbname=parsed.path.lstrip("/"),
            user=parsed.username,
            password=parsed.password,
            host=parsed.hostname,
            port=parsed.port,
        )

    connection_kwargs = {
        "dbname": DB_NAME,
        "user": DB_USER,
        "host": DB_HOST,
        "port": DB_PORT,
    }

    if DB_PASSWORD:
        connection_kwargs["password"] = DB_PASSWORD

    return psycopg2.connect(**connection_kwargs)


def ensure_core_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stations (
                id BIGSERIAL PRIMARY KEY,
                mimit_id BIGINT NOT NULL UNIQUE,
                name TEXT,
                brand TEXT,
                operator TEXT,
                address TEXT NOT NULL,
                city TEXT NOT NULL,
                province TEXT NOT NULL,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fuel_prices (
                id BIGSERIAL PRIMARY KEY,
                station_id BIGINT NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
                fuel_type TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                is_self_service BOOLEAN NOT NULL,
                reported_at TIMESTAMPTZ NOT NULL,
                source TEXT NOT NULL DEFAULT 'mimit',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_stations_mimit_id ON stations(mimit_id);
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_fuel_prices_station_id ON fuel_prices(station_id);
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_fuel_prices_fuel_type_reported_at
            ON fuel_prices(fuel_type, reported_at DESC);
            """
        )


def normalize_fuel_type(raw_fuel: str) -> str:
    value = raw_fuel.strip().lower()

    # Normalizzazione diretta (match esatti)
    mapping = {
        "benzina": "benzina",
        "super": "benzina",
        "super senza piombo": "benzina",
        "gasolio": "diesel",
        "diesel": "diesel",
        "gpl": "gpl",
        "lpg": "gpl",
        "metano": "metano",
        "gnc": "metano",
        "cng": "metano",
        "l-gnc": "metano",
        "lng": "metano",
        "gnl": "metano",
    }

    if value in mapping:
        return mapping[value]

    # HVO (diesel sintetico)
    if "hvo" in value:
        return "hvo"

    # --- Catch carburanti commerciali senza keyword standard ---
    if "v-power" in value or "verde speciale" in value:
        return "benzina_premium"

    if "hiq" in value or "perform" in value:
        return "diesel_premium"

    if "f101" in value or "f-101" in value:
        return "benzina_premium"

    # Benzina e varianti commerciali
    if "benzina" in value or "super" in value:
        if (
            "100" in value
            or "98" in value
            or "ottani" in value
            or "premium" in value
            or "v-power" in value
            or "plus" in value
            or "verde speciale" in value
            or "f101" in value
            or "f-101" in value
        ):
            return "benzina_premium"
        return "benzina"

    # Diesel / gasolio e varianti commerciali
    if "diesel" in value or "gasolio" in value:
        if (
            "premium" in value
            or "speciale" in value
            or "+" in value
            or "plus" in value
            or "v-power" in value
            or "excellium" in value
            or "hiq" in value
            or "perform" in value
        ):
            return "diesel_premium"
        return "diesel"

    # GPL
    if "gpl" in value or "lpg" in value:
        return "gpl"

    # Metano
    if any(x in value for x in ["metano", "gnc", "cng", "lng", "gnl"]):
        return "metano"

    # Fallback finale: restituisci il valore raw normalizzato, così possiamo intercettare eventuali nuovi casi
    return value


def download_file(url: str, destination: Path) -> dict[str, object]:
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    temporary_destination = destination.with_suffix(destination.suffix + ".tmp")

    with urlopen(url, context=ssl_context) as response, open(temporary_destination, "wb") as output_file:
        last_modified = response.headers.get("Last-Modified")
        shutil.copyfileobj(response, output_file)

    if not temporary_destination.exists() or temporary_destination.stat().st_size == 0:
        raise RuntimeError(f"Downloaded file is empty: {url}")

    temporary_destination.replace(destination)

    parsed_last_modified = None
    if last_modified:
        try:
            parsed_last_modified = parsedate_to_datetime(last_modified).isoformat()
        except (TypeError, ValueError):
            parsed_last_modified = last_modified

    return {
        "url": url,
        "path": str(destination),
        "size_bytes": destination.stat().st_size,
        "last_modified": parsed_last_modified,
    }


def download_latest_mimit_files() -> dict[str, object]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        anagrafica = download_file(ANAGRAFICA_URL, ANAGRAFICA_PATH)
        prezzi = download_file(PREZZI_URL, PREZZI_PATH)
    except Exception:
        anagrafica_tmp = ANAGRAFICA_PATH.with_suffix(ANAGRAFICA_PATH.suffix + ".tmp")
        prezzi_tmp = PREZZI_PATH.with_suffix(PREZZI_PATH.suffix + ".tmp")

        if anagrafica_tmp.exists():
            anagrafica_tmp.unlink()
        if prezzi_tmp.exists():
            prezzi_tmp.unlink()

        raise

    return {
        "anagrafica_path": str(ANAGRAFICA_PATH),
        "prezzi_path": str(PREZZI_PATH),
        "anagrafica": anagrafica,
        "prezzi": prezzi,
    }


def load_stations_dataframe() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    fixed_rows = 0
    skipped_rows = 0

    with open(ANAGRAFICA_PATH, newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="|")

        next(reader, None)
        next(reader, None)

        for line_number, row in enumerate(reader, start=3):
            if not row:
                continue

            if len(row) < 10:
                skipped_rows += 1
                print(
                    f"Riga anagrafica saltata (colonne insufficienti) alla linea {line_number}: {row}"
                )
                continue

            if len(row) > 10:
                fixed_rows += 1

            head = row[:4]
            middle = row[4:-4]
            tail = row[-4:]

            if len(middle) < 2:
                skipped_rows += 1
                print(
                    f"Riga anagrafica saltata (middle non valido) alla linea {line_number}: {row}"
                )
                continue

            nome_impianto = middle[0].strip()
            indirizzo = " | ".join(part.strip() for part in middle[1:] if part.strip())

            rows.append(
                {
                    "idImpianto": head[0].strip(),
                    "Gestore": head[1].strip(),
                    "Bandiera": head[2].strip(),
                    "Tipo Impianto": head[3].strip(),
                    "Nome Impianto": nome_impianto,
                    "Indirizzo": indirizzo,
                    "Comune": tail[0].strip(),
                    "Provincia": tail[1].strip(),
                    "Latitudine": tail[2].strip(),
                    "Longitudine": tail[3].strip(),
                }
            )

    df = pd.DataFrame(rows)
    print(f"Righe anagrafiche corrette automaticamente: {fixed_rows}")
    print(f"Righe anagrafiche saltate: {skipped_rows}")
    return df


def load_prices_dataframe() -> pd.DataFrame:
    df = pd.read_csv(
        PREZZI_PATH,
        sep="|",
        skiprows=1,
        encoding="utf-8",
    )

    df.columns = df.columns.str.strip()
    return df


def import_stations(conn, df_stations: pd.DataFrame) -> dict[int, int]:
    station_id_map: dict[int, int] = {}
    skipped_missing_coords = 0
    skipped_missing_address = 0

    with conn.cursor() as cur:
        for _, row in df_stations.iterrows():
            latitude_raw = str(row["Latitudine"]).strip() if pd.notna(row["Latitudine"]) else ""
            longitude_raw = str(row["Longitudine"]).strip() if pd.notna(row["Longitudine"]) else ""

            if not latitude_raw or not longitude_raw:
                skipped_missing_coords += 1
                continue

            mimit_id = int(row["idImpianto"])
            operator = str(row["Gestore"]).strip() if pd.notna(row["Gestore"]) else None
            brand = str(row["Bandiera"]).strip() if pd.notna(row["Bandiera"]) else None
            name = str(row["Nome Impianto"]).strip() if pd.notna(row["Nome Impianto"]) else None
            address = str(row["Indirizzo"]).strip() if pd.notna(row["Indirizzo"]) else ""
            city = str(row["Comune"]).strip() if pd.notna(row["Comune"]) else ""
            province = str(row["Provincia"]).strip() if pd.notna(row["Provincia"]) else ""

            if not address or not city or not province:
                skipped_missing_address += 1
                continue

            latitude = float(latitude_raw)
            longitude = float(longitude_raw)

            cur.execute(
                """
                INSERT INTO stations (
                    mimit_id, name, brand, operator, address, city, province,
                    latitude, longitude, is_active
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
                ON CONFLICT (mimit_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    brand = EXCLUDED.brand,
                    operator = EXCLUDED.operator,
                    address = EXCLUDED.address,
                    city = EXCLUDED.city,
                    province = EXCLUDED.province,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    is_active = TRUE,
                    updated_at = NOW()
                RETURNING id;
                """,
                (
                    mimit_id,
                    name,
                    brand,
                    operator,
                    address,
                    city,
                    province,
                    latitude,
                    longitude,
                ),
            )

            station_db_id = cur.fetchone()[0]
            station_id_map[mimit_id] = station_db_id

    print(f"Stazioni saltate per coordinate mancanti: {skipped_missing_coords}")
    print(f"Stazioni saltate per indirizzo/comune/provincia mancanti: {skipped_missing_address}")
    return station_id_map


def clear_prices(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM fuel_prices;")


def import_prices(conn, df_prices: pd.DataFrame, station_id_map: dict[int, int]) -> int:
    inserted_count = 0

    with conn.cursor() as cur:
        for _, row in df_prices.iterrows():
            mimit_id = int(row["idImpianto"])

            if mimit_id not in station_id_map:
                continue

            raw_fuel = str(row["descCarburante"]).strip()
            fuel_type = normalize_fuel_type(raw_fuel)

            try:
                price = float(row["prezzo"])
            except (ValueError, TypeError):
                continue

            is_self_service = str(row["isSelf"]).strip() == "1"
            reported_at = pd.to_datetime(
                str(row["dtComu"]).strip(),
                format="%d/%m/%Y %H:%M:%S",
            ).to_pydatetime()

            station_id = station_id_map[mimit_id]

            cur.execute(
                """
                INSERT INTO fuel_prices (
                    station_id, fuel_type, price, is_self_service, reported_at, source
                )
                VALUES (%s, %s, %s, %s, %s, 'mimit');
                """,
                (
                    station_id,
                    fuel_type,
                    price,
                    is_self_service,
                    reported_at,
                ),
            )

            inserted_count += 1

    return inserted_count


def import_local_mimit_files() -> dict[str, int]:
    conn = get_connection()

    try:
        print("Verifica/creazione schema database...")
        ensure_core_schema(conn)

        print("Caricamento anagrafica...")
        df_stations = load_stations_dataframe()
        if df_stations.empty:
            raise RuntimeError("Anagrafica CSV vuota o non valida")
        print(f"Stazioni lette dal CSV: {len(df_stations)}")

        print("Caricamento prezzi...")
        df_prices = load_prices_dataframe()
        if df_prices.empty:
            raise RuntimeError("Prezzi CSV vuoto o non valido")
        print(f"Prezzi letti dal CSV: {len(df_prices)}")

        print("Import stazioni nel database...")
        station_id_map = import_stations(conn, df_stations)
        if not station_id_map:
            raise RuntimeError("Nessuna stazione importata: import interrotto")
        print(f"Stazioni importate/aggiornate: {len(station_id_map)}")

        print("Pulizia prezzi esistenti...")
        clear_prices(conn)

        print("Import prezzi nel database...")
        imported_prices = import_prices(conn, df_prices, station_id_map)
        if imported_prices == 0:
            raise RuntimeError("Nessun prezzo importato: import interrotto")
        print(f"Prezzi importati: {imported_prices}")

        conn.commit()
        print("Import completato con successo.")

        return {
            "stations_csv": len(df_stations),
            "prices_csv": len(df_prices),
            "stations_imported": len(station_id_map),
            "prices_imported": imported_prices,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_mimit_data(download: bool = True) -> dict[str, object]:
    result: dict[str, object] = {}

    if download:
        print("Download dei file MIMIT in corso...")
        result["download"] = download_latest_mimit_files()
        print("Download completato.")

    result["import"] = import_local_mimit_files()
    return result


if __name__ == "__main__":
    summary = update_mimit_data(download=False)
    print(summary)
