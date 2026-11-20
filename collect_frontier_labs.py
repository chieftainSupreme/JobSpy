"""Pull all open jobs for frontier AI labs from Greenhouse and Ashby.

These companies rarely post a complete catalog on Indeed. Public ATS
board APIs return every listed job with no 1,000-result search cap.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from html import unescape
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import duckdb
import pandas as pd

from collect_data_jobs import connect, ensure_contract_columns, upsert_by_id

LABS = [
    {"company": "Anthropic", "domain": "anthropic.com", "ats": "greenhouse", "slug": "anthropic"},
    {"company": "OpenAI", "domain": "openai.com", "ats": "ashby", "slug": "openai"},
    {"company": "xAI", "domain": "x.ai", "ats": "greenhouse", "slug": "xai"},
    {"company": "Cohere", "domain": "cohere.com", "ats": "ashby", "slug": "cohere"},
    {"company": "Perplexity", "domain": "perplexity.ai", "ats": "ashby", "slug": "perplexity"},
    {"company": "Together AI", "domain": "together.ai", "ats": "greenhouse", "slug": "togetherai"},
    {"company": "Scale AI", "domain": "scale.com", "ats": "greenhouse", "slug": "scaleai"},
    {"company": "Cerebras", "domain": "cerebras.ai", "ats": "ashby", "slug": "cerebras"},
    {"company": "Character.AI", "domain": "character.ai", "ats": "ashby", "slug": "character"},
    {"company": "Inflection", "domain": "inflection.ai", "ats": "greenhouse", "slug": "inflectionai"},
    {"company": "Thinking Machines Lab", "domain": "thinkingmachines.ai", "ats": "ashby", "slug": "thinkingmachines"},
    {"company": "Figure", "domain": "figure.ai", "ats": "greenhouse", "slug": "figureai"},
    {"company": "Cognition", "domain": "cognition.ai", "ats": "ashby", "slug": "cognition"},
    {"company": "Physical Intelligence", "domain": "physicalintelligence.company", "ats": "ashby", "slug": "physicalintelligence"},
    {"company": "Liquid AI", "domain": "liquid.ai", "ats": "ashby", "slug": "liquid-ai"},
    {"company": "Fireworks AI", "domain": "fireworks.ai", "ats": "ashby", "slug": "fireworks"},
    {"company": "Stability AI", "domain": "stability.ai", "ats": "greenhouse", "slug": "stabilityai"},
    {"company": "Imbue", "domain": "imbue.com", "ats": "greenhouse", "slug": "imbue"},
    {"company": "Runway", "domain": "runwayml.com", "ats": "ashby", "slug": "runway"},
]

TAG = re.compile(r"<[^>]+>")


def fetch_json(url: str) -> dict | list:
    request = Request(url, headers={"User-Agent": "Mozilla/5.0 JobSpy frontier-labs"})
    with urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def strip_html(text: str | None) -> str | None:
    if not text:
        return None
    return unescape(TAG.sub(" ", text)).replace("\xa0", " ").strip() or None


def greenhouse_jobs(slug: str) -> list[dict]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
    payload = fetch_json(url)
    return list(payload.get("jobs") or [])


def ashby_jobs(slug: str) -> list[dict]:
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true"
    payload = fetch_json(url)
    return list(payload.get("jobs") or [])


def pay(comp: dict | None) -> tuple[float | None, float | None, str | None]:
    if not comp:
        return None, None, None
    for component in comp.get("summaryComponents") or []:
        if component.get("compensationType") == "Salary":
            return (
                component.get("minValue"),
                component.get("maxValue"),
                component.get("currencyCode"),
            )
    return None, None, None


def normalize_greenhouse(lab: dict, job: dict) -> dict:
    location = (job.get("location") or {}).get("name")
    departments = job.get("departments") or []
    return {
        "id": f"gh-{lab['slug']}-{job.get('id')}",
        "site": "greenhouse",
        "company": lab["company"],
        "domain": lab["domain"],
        "title": job.get("title"),
        "location": location,
        "department": departments[0]["name"] if departments else None,
        "job_url": job.get("absolute_url"),
        "description": strip_html(job.get("content")),
        "is_remote": bool(location and "remote" in location.lower()),
        "min_amount": None,
        "max_amount": None,
        "currency": None,
        "published_at": job.get("first_published") or job.get("updated_at"),
        "scraped_at": datetime.now(timezone.utc),
    }


def normalize_ashby(lab: dict, job: dict) -> dict:
    min_amount, max_amount, currency = pay(job.get("compensation"))
    return {
        "id": f"ashby-{lab['slug']}-{job.get('id')}",
        "site": "ashby",
        "company": lab["company"],
        "domain": lab["domain"],
        "title": job.get("title"),
        "location": job.get("location"),
        "department": job.get("department") or job.get("team"),
        "job_url": job.get("jobUrl") or job.get("applyUrl"),
        "description": job.get("descriptionPlain") or strip_html(job.get("descriptionHtml")),
        "is_remote": bool(job.get("isRemote")),
        "min_amount": min_amount,
        "max_amount": max_amount,
        "currency": currency,
        "published_at": job.get("publishedAt"),
        "scraped_at": datetime.now(timezone.utc),
    }


def setup(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS jobspy")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS jobspy.frontier_jobs (
            id VARCHAR,
            site VARCHAR,
            company VARCHAR,
            domain VARCHAR,
            organization_id INTEGER,
            title VARCHAR,
            location VARCHAR,
            department VARCHAR,
            job_url VARCHAR,
            description VARCHAR,
            is_remote BOOLEAN,
            min_amount DOUBLE,
            max_amount DOUBLE,
            currency VARCHAR,
            published_at VARCHAR,
            scraped_at TIMESTAMP WITH TIME ZONE,
            run_id VARCHAR,
            first_seen_at TIMESTAMP WITH TIME ZONE,
            last_seen_at TIMESTAMP WITH TIME ZONE
        )
        """
    )
    ensure_contract_columns(con, "frontier_jobs", backfill=True)


def org_ids(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    rows = con.execute(
        """
        SELECT lower(url) AS domain, organization_id
        FROM sumble_raw.organizations
        QUALIFY ROW_NUMBER() OVER (PARTITION BY lower(url) ORDER BY pulled_at DESC NULLS LAST) = 1
        """
    ).fetchall()
    return {domain: org_id for domain, org_id in rows if domain}


def upsert(con: duckdb.DuckDBPyConnection, batch: pd.DataFrame) -> None:
    upsert_by_id(con, "frontier_jobs", batch)


def pull_lab(lab: dict) -> pd.DataFrame:
    ats = lab["ats"]
    if ats == "greenhouse":
        rows = [normalize_greenhouse(lab, job) for job in greenhouse_jobs(lab["slug"])]
    elif ats == "ashby":
        rows = [normalize_ashby(lab, job) for job in ashby_jobs(lab["slug"])]
    else:
        rows = []
    return pd.DataFrame(rows)


def main() -> int:
    con = connect("lighthouse")
    setup(con)
    ids = org_ids(con)
    total = 0
    for lab in LABS:
        try:
            frame = pull_lab(lab)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            print(f"{lab['company']}: ERROR {exc}", flush=True)
            continue
        if frame.empty:
            print(f"{lab['company']}: 0 jobs", flush=True)
            continue
        frame["organization_id"] = ids.get(lab["domain"])
        upsert(con, frame)
        total += len(frame)
        print(f"{lab['company']}: {len(frame)} jobs via {lab['ats']}/{lab['slug']}", flush=True)

    summary = con.execute(
        """
        SELECT company, COUNT(*) AS jobs
        FROM jobspy.frontier_jobs
        GROUP BY 1
        ORDER BY jobs DESC
        """
    ).fetchall()
    print("\njobspy.frontier_jobs", flush=True)
    for company, jobs in summary:
        print(f"  {company}: {jobs}", flush=True)
    print(f"Total {sum(jobs for _, jobs in summary)} jobs", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
