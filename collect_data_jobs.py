"""Scrape data-related jobs from Indeed into MotherDuck.

Default grid: US + Canada + UK, last 30 days, one Indeed search per
title x country. Each search upserts into lighthouse.jobspy.jobs and
is recorded in jobspy.search_runs so a rerun can skip finished cells.
"""

from __future__ import annotations

import argparse
import json
import secrets
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

import duckdb
import pandas as pd

from jobspy import scrape_jobs
from jobspy.model import JobPost

SEARCH_TERMS = [
    "data engineer",
    "analytics engineer",
    "data scientist",
    "data analyst",
    "machine learning engineer",
    "BI engineer",
    "business intelligence",
    "data platform engineer",
    "etl developer",
    "dbt",
    "data architect",
    "snowflake engineer",
    "databricks engineer",
    "applied scientist",
    "BI developer",
    "data modeler",
    "data modeller",
    "analytics manager",
    "looker developer",
    "tableau developer",
    "power bi developer",
    "data ops",
    "mlops",
    "airflow",
    "decision scientist",
    "quantitative analyst",
    "insights analyst",
    "machine learning scientist",
    "data analytics",
    "warehouse engineer",
    "head of data",
    "director of analytics",
    "analytics consultant",
    "bigquery",
    "redshift engineer",
    "research scientist",
    "AI engineer",
    "ML engineer",
    "LLM engineer",
    "prompt engineer",
    "AI researcher",
    "NLP engineer",
    "computer vision",
    "generative AI",
    "staff data engineer",
    "principal data scientist",
    "data product manager",
    "product analyst",
    "growth analyst",
    "marketing analyst",
    "reporting analyst",
    "statistician",
    "spark engineer",
    "kafka engineer",
    "data governance",
    "data quality",
    "data visualization",
    "ml platform",
    "feature store",
    "lakehouse",
    "analytics lead",
    "head of analytics",
    "director of data",
    "data intern",
    "research engineer",
    "SQL developer",
    "data manager",
    "AI scientist",
    "business analyst",
    "applied AI",
    "data specialist",
    "ML intern",
    "data consultant",
    "analytics intern",
    "risk analyst",
    "operations analyst",
    "people analytics",
    "forecast analyst",
    "experimentation",
    "measurement scientist",
    "causal inference",
    "pricing analyst",
    "workforce analyst",
    "insights manager",
]

COUNTRIES = [
    {"label": "USA", "country_indeed": "USA"},
    {"label": "Canada", "country_indeed": "Canada"},
    {"label": "UK", "country_indeed": "UK"},
    {"label": "Australia", "country_indeed": "Australia"},
    {"label": "Ireland", "country_indeed": "Ireland"},
    {"label": "Germany", "country_indeed": "Germany"},
    {"label": "Netherlands", "country_indeed": "Netherlands"},
    {"label": "India", "country_indeed": "India"},
    {"label": "Singapore", "country_indeed": "Singapore"},
    {"label": "France", "country_indeed": "France"},
    {"label": "Mexico", "country_indeed": "Mexico"},
    {"label": "Brazil", "country_indeed": "Brazil"},
    {"label": "Japan", "country_indeed": "Japan"},
    {"label": "Spain", "country_indeed": "Spain"},
    {"label": "Italy", "country_indeed": "Italy"},
    {"label": "Israel", "country_indeed": "Israel"},
    {"label": "South Africa", "country_indeed": "South Africa"},
    {"label": "New Zealand", "country_indeed": "New Zealand"},
    {"label": "Belgium", "country_indeed": "Belgium"},
    {"label": "Poland", "country_indeed": "Poland"},
    {"label": "Switzerland", "country_indeed": "Switzerland"},
    {"label": "Sweden", "country_indeed": "Sweden"},
    {"label": "UAE", "country_indeed": "United Arab Emirates"},
    {"label": "Hong Kong", "country_indeed": "Hong Kong"},
    {"label": "South Korea", "country_indeed": "South Korea"},
    {"label": "Philippines", "country_indeed": "Philippines"},
    {"label": "Portugal", "country_indeed": "Portugal"},
    {"label": "Denmark", "country_indeed": "Denmark"},
    {"label": "Norway", "country_indeed": "Norway"},
    {"label": "Finland", "country_indeed": "Finland"},
    {"label": "Austria", "country_indeed": "Austria"},
    {"label": "Czech Republic", "country_indeed": "Czech Republic"},
    {"label": "Greece", "country_indeed": "Greece"},
    {"label": "Romania", "country_indeed": "Romania"},
    {"label": "Malaysia", "country_indeed": "Malaysia"},
    {"label": "Indonesia", "country_indeed": "Indonesia"},
    {"label": "Taiwan", "country_indeed": "Taiwan"},
    {"label": "Thailand", "country_indeed": "Thailand"},
    {"label": "Argentina", "country_indeed": "Argentina"},
    {"label": "Chile", "country_indeed": "Chile"},
    {"label": "Colombia", "country_indeed": "Colombia"},
    {"label": "Saudi Arabia", "country_indeed": "Saudi Arabia"},
    {"label": "Nigeria", "country_indeed": "Nigeria"},
    {"label": "Vietnam", "country_indeed": "Vietnam"},
    {"label": "Egypt", "country_indeed": "Egypt"},
    {"label": "Hungary", "country_indeed": "Hungary"},
    {"label": "Luxembourg", "country_indeed": "Luxembourg"},
    {"label": "Morocco", "country_indeed": "Morocco"},
    {"label": "Pakistan", "country_indeed": "Pakistan"},
    {"label": "Peru", "country_indeed": "Peru"},
    {"label": "Qatar", "country_indeed": "Qatar"},
    {"label": "Turkey", "country_indeed": "Turkey"},
    {"label": "Ukraine", "country_indeed": "Ukraine"},
    {"label": "China", "country_indeed": "China"},
    {"label": "Costa Rica", "country_indeed": "Costa Rica"},
    {"label": "Ecuador", "country_indeed": "Ecuador"},
    {"label": "Panama", "country_indeed": "Panama"},
    {"label": "Uruguay", "country_indeed": "Uruguay"},
    {"label": "Bahrain", "country_indeed": "Bahrain"},
    {"label": "Kuwait", "country_indeed": "Kuwait"},
    {"label": "Oman", "country_indeed": "Oman"},
    {"label": "Venezuela", "country_indeed": "Venezuela"},
]

DATA_JOB_PATTERN = (
    "data engineer|analytics engineer|data scientist|data analyst|"
    "machine learning|ml engineer|mlops|applied scientist|"
    "bi engineer|bi developer|business intelligence|"
    "data platform|data architect|data model|data ops|"
    "etl |dbt|airflow|snowflake|bigquery|redshift|"
    "research scientist|ai engineer|llm engineer|prompt engineer|"
    "nlp engineer|computer vision|generative ai|product analyst|"
    "growth analyst|data governance|data quality|lakehouse|"
    "feature store|ml platform|spark engineer|kafka engineer|"
    "databricks|looker|tableau|power bi|warehouse engineer|"
    "decision scientist|quantitative analyst|insights analyst|"
    "head of data|director of analytics|analytics"
)

JOB_COLUMNS = list(JobPost.model_fields)
META_COLUMNS = ["search_term", "search_country", "scraped_at"]
ALL_COLUMNS = JOB_COLUMNS + META_COLUMNS
FLOAT_COLUMNS = {"min_amount", "max_amount", "company_rating"}
INT_COLUMNS = {"company_reviews_count", "vacancy_count"}
BOOL_COLUMNS = {"is_remote"}
DATE_COLUMNS = {"date_posted"}
TIMESTAMP_COLUMNS = {"scraped_at"}

JOBS_DDL = """
CREATE TABLE IF NOT EXISTS jobspy.jobs (
    id VARCHAR,
    site VARCHAR,
    job_url VARCHAR,
    job_url_direct VARCHAR,
    title VARCHAR,
    company VARCHAR,
    location VARCHAR,
    date_posted DATE,
    job_type VARCHAR,
    salary_source VARCHAR,
    interval VARCHAR,
    min_amount DOUBLE,
    max_amount DOUBLE,
    currency VARCHAR,
    is_remote BOOLEAN,
    job_level VARCHAR,
    job_function VARCHAR,
    listing_type VARCHAR,
    emails VARCHAR,
    description VARCHAR,
    company_industry VARCHAR,
    company_url VARCHAR,
    company_logo VARCHAR,
    company_url_direct VARCHAR,
    company_addresses VARCHAR,
    company_num_employees VARCHAR,
    company_revenue VARCHAR,
    company_description VARCHAR,
    skills VARCHAR,
    experience_range VARCHAR,
    company_rating DOUBLE,
    company_reviews_count BIGINT,
    vacancy_count BIGINT,
    work_from_home_type VARCHAR,
    search_term VARCHAR,
    search_country VARCHAR,
    scraped_at TIMESTAMP WITH TIME ZONE,
    run_id VARCHAR,
    first_seen_at TIMESTAMP WITH TIME ZONE,
    last_seen_at TIMESTAMP WITH TIME ZONE
)
"""


def _text_cell(value: object) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "none", "nat"}:
        return None
    return str(value)


def flatten_cell(value: object) -> object:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, dict):
        country = value.get("country")
        if isinstance(country, Enum):
            country = country.value
        elif isinstance(country, dict):
            country = country.get("value")
        parts = [value.get("city"), value.get("state"), country]
        return ", ".join(str(part) for part in parts if part) or None
    if isinstance(value, list):
        texts = []
        for item in value:
            if isinstance(item, Enum):
                texts.append(item.value)
            else:
                texts.append(str(item))
        return ", ".join(texts) or None
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value
    return value


def normalize(jobs: pd.DataFrame, term: str, country: str) -> pd.DataFrame:
    frame = jobs.copy()
    for column in JOB_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
        frame[column] = frame[column].map(flatten_cell)
        if column in FLOAT_COLUMNS:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        elif column in INT_COLUMNS:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
        elif column in BOOL_COLUMNS:
            frame[column] = frame[column].map(
                lambda value: None if value is None else bool(value)
            )
        elif column in DATE_COLUMNS:
            frame[column] = pd.to_datetime(frame[column], errors="coerce").dt.date
        else:
            frame[column] = frame[column].map(_text_cell)
    frame["search_term"] = term
    frame["search_country"] = country
    frame["scraped_at"] = datetime.now(timezone.utc)
    return null_missing(frame[ALL_COLUMNS])


def connect(database: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(f"md:{database}")


def setup(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS jobspy")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS jobspy.search_runs (
            search_term VARCHAR,
            search_country VARCHAR,
            site VARCHAR,
            started_at TIMESTAMP WITH TIME ZONE,
            finished_at TIMESTAMP WITH TIME ZONE,
            rows_found INTEGER,
            rows_upserted INTEGER,
            error VARCHAR,
            PRIMARY KEY (search_term, search_country, site)
        )
        """
    )
    jobs_exists = con.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'jobspy' AND table_name = 'jobs'
        """
    ).fetchone()
    if jobs_exists and jobs_exists[0]:
        columns = {
            row[0]
            for row in con.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'jobspy' AND table_name = 'jobs'
                """
            ).fetchall()
        }
        emails_type = con.execute(
            """
            SELECT data_type FROM information_schema.columns
            WHERE table_schema = 'jobspy' AND table_name = 'jobs'
              AND column_name = 'emails'
            """
        ).fetchone()
        if "search_term" not in columns or (
            emails_type and "CHAR" not in emails_type[0].upper()
        ):
            con.execute("DROP TABLE jobspy.jobs")
    con.execute(JOBS_DDL)
    for column in (
        "listing_type",
        "job_level",
        "job_function",
        "skills",
        "experience_range",
        "work_from_home_type",
        "emails",
    ):
        col_type = con.execute(
            """
            SELECT data_type FROM information_schema.columns
            WHERE table_schema = 'jobspy' AND table_name = 'jobs'
              AND column_name = ?
            """,
            [column],
        ).fetchone()
        if col_type and "CHAR" in col_type[0].upper():
            continue
        try:
            con.execute(
                f"ALTER TABLE jobspy.jobs ALTER COLUMN {column} SET DATA TYPE VARCHAR"
            )
        except Exception:
            pass
    ensure_contract_columns(con, "jobs", backfill=True)


def refresh_view(con: duckdb.DuckDBPyConnection) -> None:
    jobs_exists = con.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'jobspy' AND table_name = 'jobs'
        """
    ).fetchone()
    if not jobs_exists or not jobs_exists[0]:
        return
    con.execute(
        f"""
        CREATE OR REPLACE VIEW jobspy.data_jobs AS
        SELECT *
        FROM jobspy.jobs
        WHERE regexp_matches(
            lower(coalesce(title, '') || ' ' || coalesce(description, '')),
            '{DATA_JOB_PATTERN}'
        )
        """
    )


CONTRACT_TABLES = ("jobs", "org_jobs", "ats_jobs", "frontier_jobs")
_CONTRACT_READY: set[str] = set()
_RUN_ID: str | None = None


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def new_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{secrets.token_hex(3)}"


def current_run_id() -> str:
    global _RUN_ID
    if _RUN_ID is None:
        _RUN_ID = new_run_id()
    return _RUN_ID


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        text = value.strip()
        return text == "" or text.lower() in {"nan", "none", "nat"}
    if isinstance(value, bool):
        return False
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def null_missing(frame: pd.DataFrame) -> pd.DataFrame:
    """Missing values are SQL NULL, never the text 'nan', 'None', or ''."""
    cleaned = frame.copy()
    for column in cleaned.columns:
        cleaned[column] = cleaned[column].map(
            lambda value: None if _is_missing(value) else value
        )
    return cleaned


def ensure_contract_columns(
    con: duckdb.DuckDBPyConnection, table: str, *, backfill: bool = False
) -> None:
    """Add lifecycle columns. Backfill only fills rows that do not have them yet."""
    if table not in CONTRACT_TABLES:
        raise ValueError(table)
    if table not in _CONTRACT_READY:
        for name, col_type in (
            ("run_id", "VARCHAR"),
            ("first_seen_at", "TIMESTAMP WITH TIME ZONE"),
            ("last_seen_at", "TIMESTAMP WITH TIME ZONE"),
        ):
            con.execute(
                f"ALTER TABLE jobspy.{_q(table)} ADD COLUMN IF NOT EXISTS {_q(name)} {col_type}"
            )
        _CONTRACT_READY.add(table)
    if not backfill:
        return
    con.execute(
        f"""
        UPDATE jobspy.{_q(table)}
        SET {_q("first_seen_at")} = COALESCE({_q("first_seen_at")}, {_q("scraped_at")}),
            {_q("last_seen_at")} = COALESCE({_q("last_seen_at")}, {_q("scraped_at")}),
            {_q("run_id")} = COALESCE({_q("run_id")}, 'pre-contract')
        WHERE ({_q("first_seen_at")} IS NULL AND {_q("scraped_at")} IS NOT NULL)
           OR ({_q("last_seen_at")} IS NULL AND {_q("scraped_at")} IS NOT NULL)
           OR {_q("run_id")} IS NULL
        """
    )


def _table_columns(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'jobspy' AND table_name = ?
        ORDER BY ordinal_position
        """,
        [table],
    ).fetchall()
    return [row[0] for row in rows]


def upsert_by_id(
    con: duckdb.DuckDBPyConnection,
    table: str,
    batch: pd.DataFrame,
    run_id: str | None = None,
) -> int:
    """Update a posting in place. first_seen_at stays; last_seen_at moves."""
    if batch is None or batch.empty:
        return 0
    ensure_contract_columns(con, table, backfill=False)
    frame = null_missing(batch)
    if "id" not in frame.columns:
        raise ValueError(f"{table} batch has no id")
    frame = frame.loc[~frame["id"].map(_is_missing)].copy()
    if frame.empty:
        return 0
    frame = frame.drop_duplicates(subset=["id"], keep="last")
    scraped = frame["scraped_at"] if "scraped_at" in frame.columns else None
    if scraped is None:
        frame["scraped_at"] = datetime.now(timezone.utc)
    else:
        frame["scraped_at"] = scraped.map(
            lambda value: datetime.now(timezone.utc) if _is_missing(value) else value
        )
    frame["run_id"] = run_id or current_run_id()
    frame["last_seen_at"] = frame["scraped_at"]
    frame["first_seen_at"] = frame["scraped_at"]
    columns = _table_columns(con, table)
    for column in columns:
        if column not in frame.columns:
            frame[column] = None
    frame = frame.loc[:, columns]
    con.register("batch", frame)
    assignments = ", ".join(
        f"{_q(column)} = batch.{_q(column)}"
        for column in columns
        if column not in {"id", "first_seen_at"}
    )
    insert_cols = ", ".join(_q(column) for column in columns)
    target = f"jobspy.{_q(table)}"
    last_exc: Exception | None = None
    for attempt in range(5):
        try:
            con.execute("BEGIN TRANSACTION")
            con.execute(
                f"""
                UPDATE {target}
                SET {assignments}
                FROM batch
                WHERE {target}.{_q("id")} = batch.{_q("id")}
                """
            )
            con.execute(
                f"""
                INSERT INTO {target} ({insert_cols})
                SELECT {insert_cols}
                FROM batch
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {target} AS existing
                    WHERE existing.{_q("id")} = batch.{_q("id")}
                )
                """
            )
            con.execute("COMMIT")
            return len(frame)
        except duckdb.Error as exc:
            try:
                con.execute("ROLLBACK")
            except duckdb.Error:
                pass
            last_exc = exc
            if "Transaction conflict" not in str(exc) or attempt == 4:
                raise
            time.sleep(1.5 * (attempt + 1))
    raise last_exc or RuntimeError("upsert failed")


def upsert(con: duckdb.DuckDBPyConnection, batch: pd.DataFrame) -> int:
    return upsert_by_id(con, "jobs", batch)


def zero_result_status(con: duckdb.DuckDBPyConnection, term: str, country: str) -> str:
    """'fresh' zeros are skipped; 'stale' zeros are rechecked after 30 days."""
    row = con.execute(
        """
        SELECT
            count(*) FILTER (
                WHERE finished_at IS NOT NULL
                  AND error IS NULL
                  AND coalesce(rows_found, 0) > 0
            ),
            epoch(max(finished_at) FILTER (
                WHERE finished_at IS NOT NULL
                  AND error IS NULL
                  AND coalesce(rows_found, 0) = 0
            ))
        FROM jobspy.search_runs
        WHERE site = 'indeed'
          AND search_term = ?
          AND search_country = ?
        """,
        [term, country],
    ).fetchone()
    hits, last_zero = row
    if hits or last_zero is None:
        return "no"
    if time.time() - float(last_zero) < 30 * 24 * 60 * 60:
        return "fresh"
    return "stale"


def already_done(con: duckdb.DuckDBPyConnection, term: str, country: str) -> bool:
    row = con.execute(
        """
        SELECT 1 FROM jobspy.search_runs
        WHERE search_term = ?
          AND search_country = ?
          AND site = 'indeed'
          AND finished_at IS NOT NULL
          AND error IS NULL
        """,
        [term, country],
    ).fetchone()
    return bool(row)


def record_start(con: duckdb.DuckDBPyConnection, term: str, country: str) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO jobspy.search_runs
            (search_term, search_country, site, started_at, finished_at,
             rows_found, rows_upserted, error)
        VALUES (?, ?, 'indeed', ?, NULL, NULL, NULL, NULL)
        """,
        [term, country, datetime.now(timezone.utc)],
    )


def record_finish(
    con: duckdb.DuckDBPyConnection,
    term: str,
    country: str,
    found: int,
    upserted: int,
    error: str | None,
) -> None:
    con.execute(
        """
        UPDATE jobspy.search_runs
        SET finished_at = ?,
            rows_found = ?,
            rows_upserted = ?,
            error = ?
        WHERE search_term = ?
          AND search_country = ?
          AND site = 'indeed'
        """,
        [datetime.now(timezone.utc), found, upserted, error, term, country],
    )


def backup(path: Path, batch: pd.DataFrame) -> None:
    if batch.empty:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    records = json.loads(batch.to_json(orient="records", date_format="iso"))
    with path.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default="lighthouse")
    parser.add_argument("--max-results", type=int, default=1000)
    parser.add_argument(
        "--hours-old",
        type=int,
        default=0,
        help="0 keeps every age. Set this only when slicing a search that hit the cap.",
    )
    parser.add_argument("--terms", nargs="*", default=SEARCH_TERMS)
    parser.add_argument(
        "--countries",
        nargs="*",
        default=[item["label"] for item in COUNTRIES],
    )
    parser.add_argument("--backup", default="data/jobs_backup.jsonl")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def scrape_cell(
    term: str,
    country_indeed: str,
    label: str,
    max_results: int,
    hours_old: int | None,
) -> pd.DataFrame:
    jobs = scrape_jobs(
        site_name=["indeed"],
        search_term=term,
        country_indeed=country_indeed,
        results_wanted=max_results,
        hours_old=hours_old,
        enforce_annual_salary=True,
        verbose=0,
    )
    errors = jobs.attrs.get("errors") or {}
    if errors:
        raise RuntimeError(str(errors))
    return normalize(jobs, term, label)


def main() -> int:
    args = parse_args()
    wanted = {name.lower() for name in args.countries}
    countries = [item for item in COUNTRIES if item["label"].lower() in wanted]
    if not countries:
        print("No matching countries", file=sys.stderr)
        return 2
    if args.workers < 1:
        print("--workers must be at least 1", file=sys.stderr)
        return 2

    con = connect(args.database)
    setup(con)
    backup_path = Path(args.backup)
    cells = [(term, country) for term in args.terms for country in countries]
    hours_old = args.hours_old if args.hours_old > 0 else None
    age_label = "any age" if hours_old is None else f"last {hours_old}h"
    print(
        f"{len(cells)} searches: {len(args.terms)} terms x {len(countries)} "
        f"countries, {age_label}, n={args.max_results}, "
        f"workers={args.workers}",
        flush=True,
    )

    pending: list[tuple[int, str, dict]] = []
    for i, (term, country) in enumerate(cells, 1):
        label = country["label"]
        prefix = f"[{i}/{len(cells)}] {term!r} / {label}"
        if not args.force:
            zeros = zero_result_status(con, term, label)
            if zeros == "fresh":
                print(f"{prefix}: skip (0 results; recheck monthly)", flush=True)
                continue
            if zeros != "stale" and already_done(con, term, label):
                print(f"{prefix}: skip (already done)", flush=True)
                continue
        record_start(con, term, label)
        pending.append((i, term, country))

    def run(item: tuple[int, str, dict]) -> tuple[int, str, str, pd.DataFrame | None, str | None]:
        index, term, country = item
        label = country["label"]
        try:
            batch = scrape_cell(
                term,
                country["country_indeed"],
                label,
                args.max_results,
                hours_old,
            )
            return index, term, label, batch, None
        except Exception as exc:  # noqa: BLE001 - keep the grid moving
            return index, term, label, None, str(exc)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(run, item) for item in pending]
        for future in as_completed(futures):
            index, term, label, batch, error = future.result()
            prefix = f"[{index}/{len(cells)}] {term!r} / {label}"
            if error or batch is None:
                record_finish(con, term, label, 0, 0, error or "empty result")
                print(f"{prefix}: ERROR {error}", flush=True)
                continue
            upserted = upsert(con, batch)
            backup(backup_path, batch)
            record_finish(con, term, label, len(batch), upserted, None)
            print(f"{prefix}: found {len(batch)}, upserted {upserted}", flush=True)

    refresh_view(con)
    jobs_exists = con.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'jobspy' AND table_name = 'jobs'
        """
    ).fetchone()
    if jobs_exists and jobs_exists[0]:
        totals = con.execute(
            """
            SELECT
                COUNT(*) AS jobs,
                COUNT(description) AS with_description,
                COUNT(DISTINCT search_country) AS countries,
                COUNT(DISTINCT search_term) AS terms
            FROM jobspy.jobs
            """
        ).fetchone()
        filtered = con.execute("SELECT COUNT(*) FROM jobspy.data_jobs").fetchone()
        print(
            f"Done. jobspy.jobs={totals[0]} descriptions={totals[1]} "
            f"countries={totals[2]} terms={totals[3]} data_jobs={filtered[0]}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
