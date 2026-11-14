"""Scrape LinkedIn, Glassdoor, and ZipRecruiter for data-job titles.

Indeed already ran for these terms. Other JobSpy sites add postings
Indeed never returned. Resume is stored in jobspy.search_runs by site.
"""

from __future__ import annotations

from datetime import datetime, timezone

from collect_data_jobs import (
    connect,
    normalize,
    setup,
    upsert,
)
from jobspy import scrape_jobs

HIGH_YIELD = [
    "data engineer",
    "data scientist",
    "machine learning engineer",
    "data analyst",
    "AI engineer",
    "research scientist",
    "analytics engineer",
    "applied scientist",
    "data architect",
    "ML engineer",
    "MLOps",
    "prompt engineer",
    "LLM engineer",
    "business intelligence",
    "quantitative analyst",
    "data analyst",
    "analytics engineer",
    "data intern",
    "product analyst",
    "growth analyst",
    "data governance",
    "ml platform",
    "staff data engineer",
    "principal data scientist",
    "NLP engineer",
    "computer vision",
    "generative AI",
    "AI researcher",
]

LOCATIONS = [
    {
        "label": "USA",
        "location": "United States",
        "country_indeed": "USA",
        "sites": ["zip_recruiter", "glassdoor", "linkedin"],
    },
    {
        "label": "Canada",
        "location": "Canada",
        "country_indeed": "Canada",
        "sites": ["glassdoor", "linkedin"],
    },
    {
        "label": "UK",
        "location": "United Kingdom",
        "country_indeed": "UK",
        "sites": ["glassdoor", "linkedin"],
    },
]


def already_done(con, term: str, country: str, site: str) -> bool:
    row = con.execute(
        """
        SELECT 1 FROM jobspy.search_runs
        WHERE search_term = ?
          AND search_country = ?
          AND site = ?
          AND finished_at IS NOT NULL
          AND error IS NULL
        """,
        [term, country, site],
    ).fetchone()
    return bool(row)


def record_start(con, term: str, country: str, site: str) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO jobspy.search_runs
            (search_term, search_country, site, started_at, finished_at,
             rows_found, rows_upserted, error)
        VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL)
        """,
        [term, country, site, datetime.now(timezone.utc)],
    )


def record_finish(
    con, term: str, country: str, site: str, found: int, upserted: int, error: str | None
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
          AND site = ?
        """,
        [datetime.now(timezone.utc), found, upserted, error, term, country, site],
    )


def main() -> int:
    con = connect("lighthouse")
    setup(con)
    cells = [
        (term, loc, site)
        for term in HIGH_YIELD
        for loc in LOCATIONS
        for site in loc["sites"]
    ]
    print(f"{len(cells)} searches across LinkedIn/Glassdoor/ZipRecruiter", flush=True)
    for i, (term, loc, site) in enumerate(cells, 1):
        label = loc["label"]
        prefix = f"[{i}/{len(cells)}] {site} {term!r} / {label}"
        if already_done(con, term, label, site):
            print(f"{prefix}: skip", flush=True)
            continue
        print(f"{prefix}: scraping...", flush=True)
        record_start(con, term, label, site)
        try:
            jobs = scrape_jobs(
                site_name=[site],
                search_term=term,
                location=loc["location"],
                country_indeed=loc["country_indeed"],
                results_wanted=1000,
                hours_old=720,
                linkedin_fetch_description=False,
                enforce_annual_salary=True,
                verbose=1,
            )
            errors = jobs.attrs.get("errors") or {}
            if errors:
                raise RuntimeError(str(errors))
            batch = normalize(jobs, term, label)
            upserted = upsert(con, batch)
            record_finish(con, term, label, site, len(batch), upserted, None)
            print(f"{prefix}: found {len(batch)}, upserted {upserted}", flush=True)
        except Exception as exc:  # noqa: BLE001
            record_finish(con, term, label, site, 0, 0, str(exc))
            print(f"{prefix}: ERROR {exc}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
