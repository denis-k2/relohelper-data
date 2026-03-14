import logging
import re
from datetime import date
from os import getenv
from time import time
from typing import Dict, List, Tuple, Optional

import pandas as pd
import psycopg2
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from psycopg2 import Error
from psycopg2.extensions import connection as PgConnection
from psycopg2.extensions import cursor as PgCursor

load_dotenv()

# =========================================================
# Configuration
# =========================================================

WEATHER_ATLAS_BASE_URL = "https://www.weather-atlas.com"
DEFAULT_TIMEOUT = 30

# This cookie is the key fix:
# Weather Atlas returns metric units when this cookie is present.
WEATHER_UNITS_COOKIE_VALUE = "c|mm|mb|km"

# A stable city page to infer climate table structure
SCHEMA_SAMPLE_URL = "https://www.weather-atlas.com/en/canada/vancouver-climate"

# SQL file that creates avg_climate table.
# Expected placeholder in file: {climate_params}
CREATE_TABLE_SQL_PATH = "./2026_2/sql/create_avg_climate.sql"

# Source dataframe with cities / links
NUMBEO_LINKS_PICKLE_PATH = "./2026_2/data/geonameid.pkl"

# Log file
LOG_FILE_PATH = "./2026_2/data/logs.log"


# =========================================================
# Logging
# =========================================================

def setup_logging() -> None:
    logging.basicConfig(
        filename=LOG_FILE_PATH,
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


# =========================================================
# HTTP session
# =========================================================

def build_weather_atlas_session() -> requests.Session:
    """
    Create a persistent HTTP session for Weather Atlas.
    The important part is the weather_units cookie.
    """
    session = requests.Session()

    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64; rv:148.0) "
                "Gecko/20100101 Firefox/148.0"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": WEATHER_ATLAS_BASE_URL + "/",
        }
    )

    # This is the main fix.
    session.cookies.set(
        "weather_units",
        WEATHER_UNITS_COOKIE_VALUE,
        domain="www.weather-atlas.com",
    )

    return session


# =========================================================
# URL construction
# =========================================================

def construct_url(country: str, state: Optional[str], city: str) -> str:
    """
    Build Weather Atlas climate URL.

    For the US, Weather Atlas uses:
    /en/<state>-usa/<city>-climate

    Examples:
    - United States / New York / New York
      -> /en/new-york-usa/new-york-climate
    - Canada / Vancouver
      -> /en/canada/vancouver-climate

    We no longer rely on ?c,mm,mb,km in the URL.
    Units are forced by cookie instead.
    """
    if country == "United States":
        state_part = (state or "").replace(" ", "-")
        country_part = f"{state_part}-usa"
    else:
        country_part = country.replace(" ", "-")

    city_part = city.replace(" ", "-")
    return f"{WEATHER_ATLAS_BASE_URL}/en/{country_part}/{city_part}-climate"


# =========================================================
# HTML parsing
# =========================================================

def fetch_html(session: requests.Session, url: str) -> str:
    response = session.get(url, allow_redirects=True, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    return response.text


def parse_city_dict_from_html(html: str) -> Dict[str, List[str]]:
    """
    Parse all climate rows from Weather Atlas page into a raw dict.

    Example raw entry before split:
    {
        "Average temperature in January": "−3.1°C"
    }

    After regex split:
    {
        "Average temperature in January": ["", "-3.1", "°C"]
    }
    """
    city_dict: Dict[str, List[str]] = {}

    soup = BeautifulSoup(html, "lxml")
    ul_tags = soup.find_all("ul", class_="list-unstyled mb-0")

    if not ul_tags:
        return city_dict

    for ul in ul_tags:
        li_tags = ul.find_all("li")
        for li in li_tags:
            if li.a and li.span:
                key = li.a.text.strip()
                value = li.span.text.strip()
                city_dict[key] = re.split(r"(\-?\d*\.?\d+|\d+)", value)

    return city_dict


def scrap_city_dict(session: requests.Session, url: str) -> Dict[str, List[str]]:
    """
    Fetch and parse one city page.
    """
    try:
        html = fetch_html(session, url)
        city_dict = parse_city_dict_from_html(html)

        if not city_dict:
            logging.warning("No climate data parsed from URL: %s", url)

        return city_dict

    except requests.RequestException as ex:
        logging.exception("HTTP error for URL %s: %s", url, ex)
        return {}
    except Exception as ex:
        logging.exception("Unexpected parsing error for URL %s: %s", url, ex)
        return {}


# =========================================================
# Climate metadata extraction
# =========================================================

def normalize_param_name(raw_param: str) -> str:
    """
    Convert Weather Atlas label to DB-safe snake_case column name.

    Example:
    'Average temperature' -> 'temperature'
    'Average rainfall' -> 'rainfall'
    """
    return (
        raw_param.removeprefix("Average ")
        .removesuffix("erature")
        .replace(" ", "_")
        .lower()
    )


def normalize_unit(raw_unit: str) -> str:
    """
    Normalize unit labels extracted from the page.
    """
    return (
        raw_unit.strip()
        .lstrip("В")       # keep your original workaround
        .replace("h", "hours")
        .replace("km/hours", "km/h")
    )


def split_label_into_param_and_month(label: str) -> Tuple[str, str]:
    """
    Example:
    'Average temperature in January'
    -> ('Average temperature', 'January')
    """
    parts = label.split(" in ")
    if len(parts) != 2:
        raise ValueError(f"Unexpected climate label format: {label}")
    return parts[0], parts[1]


def get_params_dict(city_dict: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """
    Build mapping:
    {
        'Average temperature': ['temperature', '°C'],
        'Average rainfall': ['rainfall', 'mm'],
        ...
    }
    """
    params_dict: Dict[str, List[str]] = {}

    for key, value in city_dict.items():
        raw_param, _ = split_label_into_param_and_month(key)

        if raw_param not in params_dict:
            column_name = normalize_param_name(raw_param)
            unit = normalize_unit(value[2] if len(value) > 2 else "")
            params_dict[raw_param] = [column_name, unit]

    return params_dict


def get_months_dict(city_dict: Dict[str, List[str]]) -> Dict[str, int]:
    """
    Build mapping:
    {
        'January': 1,
        'February': 2,
        ...
    }
    """
    months_dict: Dict[str, int] = {}
    count = 0

    for key in city_dict.keys():
        _, month = split_label_into_param_and_month(key)
        if month not in months_dict:
            count += 1
            months_dict[month] = count

    return months_dict


def get_columns_list(params_dict: Dict[str, List[str]]) -> List[str]:
    return [value[0] for value in params_dict.values()]


# =========================================================
# DataFrame preparation
# =========================================================

def params_template_df(months_dict: Dict[str, int], columns_list: List[str]) -> pd.DataFrame:
    """
    Create empty climate dataframe with 12 rows (months) and metric columns.
    """
    df_params_templ = pd.DataFrame(
        index=months_dict.values(),
        columns=columns_list,
        dtype=None,
    )

    df_params_templ.loc[:, :] = None
    df_params_templ.insert(0, "city_id", None)
    df_params_templ.insert(1, "month", df_params_templ.index)

    return df_params_templ


def fill_params_template_df(
    city_dict: Dict[str, List[str]],
    months_dict: Dict[str, int],
    params_dict: Dict[str, List[str]],
    df_params_fill: pd.DataFrame,
) -> pd.DataFrame:
    """
    Fill empty template dataframe with numeric climate values for one city.
    """
    for key, value in city_dict.items():
        raw_param, month_name = split_label_into_param_and_month(key)
        month_number = months_dict[month_name]
        column_name = params_dict[raw_param][0]

        if len(value) < 2 or value[1] == "":
            continue

        num = float(value[1])
        df_params_fill.loc[month_number, column_name] = num

    return df_params_fill


def build_city_climate_df(
    city_id: int,
    city_dict: Dict[str, List[str]],
    months_dict: Dict[str, int],
    params_dict: Dict[str, List[str]],
    df_template: pd.DataFrame,
    data_engr: str,
) -> pd.DataFrame:
    """
    Build final avg_climate dataframe for one city.
    """
    df_city = df_template.copy()
    df_city["city_id"] = city_id

    df_city = fill_params_template_df(
        city_dict=city_dict,
        months_dict=months_dict,
        params_dict=params_dict,
        df_params_fill=df_city,
    )

    df_city["updated_date"] = date.today()
    df_city["updated_by"] = data_engr

    return df_city


# =========================================================
# Database helpers
# =========================================================

def connect_db(db_url: str) -> PgConnection:
    return psycopg2.connect(db_url)


def create_climate_table(
    connection: PgConnection,
    cursor: PgCursor,
    params_dict: Dict[str, List[str]],
) -> None:
    """
    Create avg_climate table dynamically based on detected climate params.
    Add comments with original Weather Atlas labels and units.

    This keeps your original design but avoids globals.
    """
    climate_params_sql = ""
    comments_sql = ""

    for original_param, (column_name, unit) in params_dict.items():
        climate_params_sql += f"{column_name} numeric,"
        comments_sql += (
            f"COMMENT ON COLUMN avg_climate.{column_name} "
            f"IS '{original_param}, {unit}';"
        )

    with open(CREATE_TABLE_SQL_PATH, encoding="utf-8") as sql_script:
        sql_template = sql_script.read()

    sql_script = sql_template.format(climate_params=climate_params_sql)

    cursor.execute(sql_script)
    cursor.execute(comments_sql)
    connection.commit()


def build_insert_sql(params_dict: Dict[str, List[str]]) -> str:
    """
    Build INSERT statement dynamically.

    Assumes avg_climate has unique constraint or PK on (city_id, month).
    If your table currently does not have that constraint, add it.
    """
    metric_columns = [value[0] for value in params_dict.values()]
    all_columns = ["city_id", "month", *metric_columns, "updated_date", "updated_by"]

    columns_sql = ", ".join(all_columns)
    placeholders_sql = ", ".join(["%s"] * len(all_columns))

    update_columns = [*metric_columns, "updated_date", "updated_by"]
    update_sql = ", ".join(
        [f"{col} = EXCLUDED.{col}" for col in update_columns]
    )

    sql = f"""
        INSERT INTO avg_climate ({columns_sql})
        VALUES ({placeholders_sql})
        ON CONFLICT (city_id, month)
        DO UPDATE SET {update_sql}
    """
    return sql


def dataframe_to_tuples(df: pd.DataFrame) -> List[Tuple]:
    return [tuple(row) for row in df.itertuples(index=False, name=None)]


# =========================================================
# Main scraping pipeline
# =========================================================

def bootstrap_schema_from_sample_city(
    session: requests.Session,
    connection: PgConnection,
    cursor: PgCursor,
) -> Tuple[Dict[str, List[str]], Dict[str, int], pd.DataFrame]:
    """
    Use one known-good city page to infer table schema.
    """
    sample_city_dict = scrap_city_dict(session, SCHEMA_SAMPLE_URL)

    if not sample_city_dict:
        raise RuntimeError(
            f"Failed to infer schema from sample URL: {SCHEMA_SAMPLE_URL}"
        )

    params_dict = get_params_dict(sample_city_dict)
    months_dict = get_months_dict(sample_city_dict)
    columns_list = get_columns_list(params_dict)

    create_climate_table(connection, cursor, params_dict)

    df_template = params_template_df(months_dict, columns_list)
    return params_dict, months_dict, df_template


def load_source_dataframe() -> pd.DataFrame:
    """
    Load city source dataframe and sort it for easier debugging.
    """
    df_numbeo = pd.read_pickle(NUMBEO_LINKS_PICKLE_PATH)
    df_numbeo.sort_values("country", inplace=True)
    return df_numbeo


def scrape_all_cities_to_db() -> None:
    """
    Main end-to-end pipeline:
    - build session with metric cookie
    - load city dataframe
    - infer schema from sample page
    - scrape all cities
    - insert/update avg_climate in PostgreSQL
    """
    data_engr = getenv("DATA_ENGR")
    db_url = getenv("SQLALCHEMY_RELOHELPER_URL")

    if not data_engr:
        raise RuntimeError("DATA_ENGR environment variable is not set")

    if not db_url:
        raise RuntimeError("SQLALCHEMY_RELOHELPER_URL environment variable is not set")

    setup_logging()
    start_time = time()

    session = build_weather_atlas_session()
    df_numbeo = load_source_dataframe()

    connection: Optional[PgConnection] = None

    try:
        connection = connect_db(db_url)
        cursor = connection.cursor()

        params_dict, months_dict, df_template = bootstrap_schema_from_sample_city(
            session=session,
            connection=connection,
            cursor=cursor,
        )

        insert_sql = build_insert_sql(params_dict)

        success_count = 0
        fail_count = 0

        for city_id, row in df_numbeo.iterrows():
            url = construct_url(
                country=row["country"],
                state=row.get("state_name"),
                city=row["city"],
            )

            city_dict = scrap_city_dict(session, url)

            if not city_dict:
                fail_count += 1
                logging.info("city_id:%s wrong_url_or_empty_parse: %s", city_id, url)
                continue

            try:
                df_city = build_city_climate_df(
                    city_id=city_id,
                    city_dict=city_dict,
                    months_dict=months_dict,
                    params_dict=params_dict,
                    df_template=df_template,
                    data_engr=data_engr,
                )

                rows_to_insert = dataframe_to_tuples(df_city)
                cursor.executemany(insert_sql, rows_to_insert)
                connection.commit()

                success_count += 1

            except Exception as ex:
                connection.rollback()
                fail_count += 1
                logging.exception(
                    "Failed to insert climate data for city_id=%s, url=%s, error=%s",
                    city_id,
                    url,
                    ex,
                )

        finish_time = time()
        elapsed = finish_time - start_time

        print("[INFO] Scraping finished.")
        print(f"[INFO] Success cities: {success_count}")
        print(f"[INFO] Failed cities: {fail_count}")
        print(f"[INFO] Execution time: {elapsed:.2f} sec")

        logging.info("Finished scraping in %.2f sec", elapsed)
        logging.info("Success cities: %s", success_count)
        logging.info("Failed cities: %s", fail_count)

    except (Exception, Error) as error:
        print("[INFO Error]:", error)
        logging.exception("Fatal pipeline error: %s", error)

    finally:
        if connection:
            connection.close()
            print("[INFO] Postgres connection closed.")


# =========================================================
# Entrypoint
# =========================================================

if __name__ == "__main__":
    scrape_all_cities_to_db()