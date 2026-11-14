"""Scrape Indeed jobs for every account in sumble_raw.organizations.

Each organization is searched by company name on the Indeed site that
matches its HQ country. Results are kept only when the employer name or
website matches the organization, then upserted into jobspy.org_jobs.
"""

from __future__ import annotations

import argparse
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlparse

import duckdb
import pandas as pd

from collect_data_jobs import ALL_COLUMNS, connect, normalize
from jobspy import scrape_jobs
from jobspy.model import Country

# JobSpy requires results_wanted >= 1. This is not a product cap; Indeed
# stops earlier when it returns no next cursor (often around 1000).
UNTIL_EXHAUSTED = 100_000

LEGAL_SUFFIX = re.compile(
    r"\b(the|company|companies|inc|incorporated|llc|ltd|limited|"
    r"corp|corporation|co|plc|gmbh|ag|sa|pty)\b",
    re.I,
)


def domain_root(value: str | None) -> str:
    if not value:
        return ""
    text = value.strip().lower()
    if "://" not in text:
        text = "http://" + text
    host = urlparse(text).hostname or ""
    if host.startswith("www."):
        host = host[4:]
    return host


def search_name(name: str) -> str:
    cleaned = LEGAL_SUFFIX.sub(" ", name)
    cleaned = re.sub(r"[.,]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" &-")
    return cleaned or name


def indeed_country(raw: str | None) -> str:
    if not raw:
        return "usa"
    try:
        return Country(raw.strip().lower()).value
    except ValueError:
        return "usa"


# Names this short are too common for "contains" matching (Ramp, Air, Kit).
WORD_MATCH_MIN = 6

# Extra employer names that should count as the org even when they don't
# share a prefix. Add more rows to jobspy.org_aliases anytime.
ALIAS_SEEDS = {
    "aramco": [
        "saudi aramco",
        "saudi arabian oil",
        "saudi arabian oil company",
    ],
}


def contains_word(text: str, needle: str) -> bool:
    if not needle or len(needle) < WORD_MATCH_MIN:
        return False
    return re.search(rf"\b{re.escape(needle)}\b", text) is not None


def job_matches_org(
    row: pd.Series,
    name: str,
    domain: str,
    aliases: list[str] | None = None,
) -> bool:
    company = str(row.get("company") or "").strip().lower()
    org = name.strip().lower()
    direct = domain_root(str(row.get("company_url_direct") or ""))
    needles = [org, search_name(name).lower(), *(a.lower() for a in aliases or [])]
    needles = [needle for needle in dict.fromkeys(needles) if needle]

    if domain and direct:
        if domain == direct or direct.endswith("." + domain) or domain.endswith("." + direct):
            return True
        host = direct.split(".")[0]
        if len(org) >= WORD_MATCH_MIN and org in host.replace("-", ""):
            return True

    for needle in needles:
        if company == needle:
            return True
        if company.startswith(needle + " ") or company.startswith(needle + ","):
            return True
        if contains_word(company, needle):
            return True
    return False


def load_orgs(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(
        """
        SELECT
            o.organization_id,
            o.name,
            o.url AS domain,
            f.country
        FROM sumble_raw.organizations o
        LEFT JOIN organization_firmographics f
          ON o.organization_id = f.organization_id
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY o.organization_id
            ORDER BY o.pulled_at DESC NULLS LAST
        ) = 1
        """
    ).df()


def setup(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS jobspy")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS jobspy.org_aliases (
            organization_id INTEGER,
            alias VARCHAR,
            PRIMARY KEY (organization_id, alias)
        )
        """
    )
    for org_name, aliases in ALIAS_SEEDS.items():
        for alias in aliases:
            con.execute(
                """
                INSERT OR IGNORE INTO jobspy.org_aliases (organization_id, alias)
                SELECT organization_id, ?
                FROM sumble_raw.organizations
                WHERE lower(name) = ?
                """,
                [alias, org_name],
            )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS jobspy.org_search_runs (
            organization_id INTEGER PRIMARY KEY,
            organization_name VARCHAR,
            country VARCHAR,
            started_at TIMESTAMP WITH TIME ZONE,
            finished_at TIMESTAMP WITH TIME ZONE,
            rows_found INTEGER,
            rows_matched INTEGER,
            error VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS jobspy.org_jobs (
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
            organization_id INTEGER,
            organization_name VARCHAR,
            organization_domain VARCHAR
        )
        """
    )


def already_done(con: duckdb.DuckDBPyConnection, org_id: int) -> bool:
    row = con.execute(
        """
        SELECT 1 FROM jobspy.org_search_runs
        WHERE organization_id = ?
          AND finished_at IS NOT NULL
          AND error IS NULL
        """,
        [org_id],
    ).fetchone()
    return bool(row)


write_lock = threading.Lock()


def upsert_org_jobs(con: duckdb.DuckDBPyConnection, batch: pd.DataFrame) -> int:
    if batch.empty:
        return 0
    with write_lock:
        con.register("batch", batch)
        con.execute(
            """
            DELETE FROM jobspy.org_jobs
            WHERE id IN (SELECT id FROM batch WHERE id IS NOT NULL)
               OR (organization_id, id) IN (
                    SELECT organization_id, id FROM batch
               )
            """
        )
        con.execute("INSERT INTO jobspy.org_jobs BY NAME SELECT * FROM batch")
    return len(batch)


def record_start(con: duckdb.DuckDBPyConnection, org_id: int, name: str, country: str) -> None:
    with write_lock:
        con.execute(
            """
            INSERT OR REPLACE INTO jobspy.org_search_runs
                (organization_id, organization_name, country, started_at,
                 finished_at, rows_found, rows_matched, error)
            VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL)
            """,
            [org_id, name, country, datetime.now(timezone.utc)],
        )


def record_finish(
    con: duckdb.DuckDBPyConnection,
    org_id: int,
    found: int,
    matched: int,
    error: str | None,
) -> None:
    with write_lock:
        con.execute(
            """
            UPDATE jobspy.org_search_runs
            SET finished_at = ?,
                rows_found = ?,
                rows_matched = ?,
                error = ?
            WHERE organization_id = ?
            """,
            [datetime.now(timezone.utc), found, matched, error, org_id],
        )


def scrape_org(
    con: duckdb.DuckDBPyConnection,
    org: pd.Series,
    max_results: int,
) -> tuple[str, int, int, str | None]:
    org_id = int(org["organization_id"])
    name = str(org["name"])
    domain = domain_root(org["domain"] if pd.notna(org["domain"]) else "")
    country = indeed_country(org["country"] if pd.notna(org["country"]) else None)
    query = search_name(name)
    with write_lock:
        alias_rows = con.execute(
            "SELECT alias FROM jobspy.org_aliases WHERE organization_id = ?",
            [org_id],
        ).fetchall()
    aliases = [row[0] for row in alias_rows]
    record_start(con, org_id, name, country)
    try:
        frames: list[pd.DataFrame] = []
        last_error = None
        queries = [query, *(search_name(alias) for alias in aliases)]
        queries = list(dict.fromkeys(q for q in queries if q))
        for search in queries:
            jobs = None
            for attempt in range(1, 4):
                jobs = scrape_jobs(
                    site_name=["indeed"],
                    search_term=search,
                    country_indeed=country,
                    results_wanted=max_results,
                    verbose=0,
                )
                errors = (jobs.attrs or {}).get("errors") or {}
                if not errors:
                    last_error = None
                    break
                last_error = str(errors)
                time.sleep(2 * attempt)
            if jobs is not None and not jobs.empty:
                frames.append(jobs)
        if not frames:
            record_finish(con, org_id, 0, 0, last_error)
            return name, 0, 0, last_error
        jobs = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["id"])
        found = len(jobs)
        batch = normalize(jobs, query, country)
        matched = batch[
            batch.apply(
                lambda row: job_matches_org(row, name, domain, aliases),
                axis=1,
            )
        ].copy()
        matched["organization_id"] = org_id
        matched["organization_name"] = name
        matched["organization_domain"] = domain or None
        upserted = upsert_org_jobs(con, matched)
        record_finish(con, org_id, found, upserted, last_error)
        return name, found, upserted, last_error
    except Exception as exc:  # noqa: BLE001
        record_finish(con, org_id, 0, 0, str(exc))
        return name, 0, 0, str(exc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default="lighthouse")
    parser.add_argument(
        "--max-results",
        type=int,
        default=0,
        help="0 = page until Indeed has no more results (no per-company cap)",
    )
    parser.add_argument("--limit", type=int, default=0, help="process only the first N orgs")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    con = connect(args.database)
    setup(con)
    orgs = load_orgs(con)
    if not args.force:
        done = {
            row[0]
            for row in con.execute(
                """
                SELECT organization_id FROM jobspy.org_search_runs
                WHERE finished_at IS NOT NULL AND error IS NULL
                """
            ).fetchall()
        }
        orgs = orgs[~orgs["organization_id"].isin(done)]
    if args.limit:
        orgs = orgs.head(args.limit)
    if orgs.empty:
        print("No organizations left to scrape", flush=True)
        return 0

    wanted = args.max_results if args.max_results > 0 else UNTIL_EXHAUSTED
    cap_label = "until Indeed stops" if args.max_results <= 0 else str(wanted)
    print(
        f"Scraping {len(orgs)} organizations on Indeed, "
        f"n={cap_label}, workers={args.workers}",
        flush=True,
    )
    completed = 0
    matched_total = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(scrape_org, con, org, wanted)
            for _, org in orgs.iterrows()
        ]
        for future in as_completed(futures):
            name, found, matched, error = future.result()
            completed += 1
            matched_total += matched
            prefix = f"[{completed}/{len(orgs)}] {name}"
            if error:
                print(f"{prefix}: ERROR {error}", flush=True)
            else:
                print(
                    f"{prefix}: found {found}, matched {matched}",
                    flush=True,
                )

    totals = con.execute(
        """
        SELECT COUNT(*) AS jobs,
               COUNT(DISTINCT organization_id) AS orgs_with_jobs
        FROM jobspy.org_jobs
        """
    ).fetchone()
    runs = con.execute(
        """
        SELECT COUNT(*) AS finished,
               COUNT(*) FILTER (WHERE coalesce(rows_matched,0) > 0) AS with_matches
        FROM jobspy.org_search_runs
        WHERE finished_at IS NOT NULL AND error IS NULL
        """
    ).fetchone()
    print(
        f"Done. org_jobs={totals[0]} orgs_with_jobs={totals[1]} "
        f"finished_orgs={runs[0]} orgs_with_indeed_jobs={runs[1]} "
        f"matched_this_run={matched_total}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
