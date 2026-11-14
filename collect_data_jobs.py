"""Scrape data-related jobs from Indeed into MotherDuck.

Default grid: US + Canada + UK, last 30 days, one Indeed search per
title x country. Each search upserts into lighthouse.jobspy.jobs and
is recorded in jobspy.search_runs so a rerun can skip finished cells.
"""

from __future__ import annotations

import argparse
import json
import sys
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
]

COUNTRIES = [
    {"label": "USA", "country_indeed": "USA"},
    {"label": "Canada", "country_indeed": "Canada"},
    {"label": "UK", "country_indeed": "UK"},
]

DATA_JOB_PATTERN = (
    "data engineer|analytics engineer|data scientist|data analyst|"
    "machine learning|ml engineer|mlops|applied scientist|"
    "bi engineer|bi developer|business intelligence|"
    "data platform|data architect|data model|data ops|"
    "etl |dbt|airflow|snowflake|bigquery|redshift|"
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
    scraped_at TIMESTAMP WITH TIME ZONE
)
"""


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
            frame[column] = frame[column].map(
                lambda value: None if value is None else str(value)
            )
    frame["search_term"] = term
    frame["search_country"] = country
    frame["scraped_at"] = datetime.now(timezone.utc)
    return frame[ALL_COLUMNS]


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


def upsert(con: duckdb.DuckDBPyConnection, batch: pd.DataFrame) -> int:
    if batch.empty:
        return 0
    con.register("batch", batch)
    con.execute(
        """
        DELETE FROM jobspy.jobs
        WHERE id IN (SELECT id FROM batch WHERE id IS NOT NULL)
        """
    )
    con.execute("INSERT INTO jobspy.jobs BY NAME SELECT * FROM batch")
    return len(batch)


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
    parser.add_argument("--hours-old", type=int, default=720)
    parser.add_argument("--terms", nargs="*", default=SEARCH_TERMS)
    parser.add_argument(
        "--countries",
        nargs="*",
        default=[item["label"] for item in COUNTRIES],
    )
    parser.add_argument("--backup", default="data/jobs_backup.jsonl")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    wanted = {name.lower() for name in args.countries}
    countries = [item for item in COUNTRIES if item["label"].lower() in wanted]
    if not countries:
        print("No matching countries", file=sys.stderr)
        return 2

    con = connect(args.database)
    setup(con)
    backup_path = Path(args.backup)
    cells = [(term, country) for term in args.terms for country in countries]
    print(
        f"{len(cells)} searches: {len(args.terms)} terms x {len(countries)} "
        f"countries, last {args.hours_old}h, n={args.max_results}",
        flush=True,
    )

    for i, (term, country) in enumerate(cells, 1):
        label = country["label"]
        prefix = f"[{i}/{len(cells)}] {term!r} / {label}"
        if not args.force and already_done(con, term, label):
            print(f"{prefix}: skip (already done)", flush=True)
            continue
        print(f"{prefix}: scraping...", flush=True)
        record_start(con, term, label)
        try:
            jobs = scrape_jobs(
                site_name=["indeed"],
                search_term=term,
                country_indeed=country["country_indeed"],
                results_wanted=args.max_results,
                hours_old=args.hours_old,
                enforce_annual_salary=True,
                verbose=1,
            )
            errors = jobs.attrs.get("errors") or {}
            if errors:
                raise RuntimeError(str(errors))
            batch = normalize(jobs, term, label)
            upserted = upsert(con, batch)
            backup(backup_path, batch)
            record_finish(con, term, label, len(batch), upserted, None)
            print(f"{prefix}: found {len(batch)}, upserted {upserted}", flush=True)
        except Exception as exc:  # noqa: BLE001 - keep the grid moving
            record_finish(con, term, label, 0, 0, str(exc))
            print(f"{prefix}: ERROR {exc}", flush=True)

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
