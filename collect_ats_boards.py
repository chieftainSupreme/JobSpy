"""Fetch every job from Greenhouse, Ashby, and Lever boards in ats_raw.boards.

Public ATS APIs return the full catalog with no Indeed-style 1,000 cap.
"""

from __future__ import annotations

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html import unescape
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd

from collect_data_jobs import connect, ensure_contract_columns, upsert_by_id
from collect_frontier_labs import (
    ashby_jobs,
    fetch_json as frontier_fetch_json,
    normalize_ashby,
    normalize_greenhouse,
    org_ids,
    strip_html,
)


def greenhouse_jobs(slug: str) -> list[dict]:
    # List endpoint (no content=true) returns every posting; descriptions
    # can be filled later. This avoids timing out on 2,000+ job boards.
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    payload = frontier_fetch_json(url)
    return list(payload.get("jobs") or [])

TAG = re.compile(r"<[^>]+>")


def fetch_json(url: str) -> dict | list:
    request = Request(url, headers={"User-Agent": "Mozilla/5.0 JobSpy ats-boards"})
    with urlopen(request, timeout=90) as response:
        return json.loads(response.read().decode("utf-8"))


def lever_jobs(slug: str) -> list[dict]:
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    payload = fetch_json(url)
    return list(payload) if isinstance(payload, list) else []


def normalize_lever(lab: dict, job: dict) -> dict:
    cats = job.get("categories") or {}
    return {
        "id": f"lever-{lab['slug']}-{job.get('id')}",
        "site": "lever",
        "company": lab["company"],
        "domain": lab["domain"],
        "title": job.get("text"),
        "location": cats.get("location") or job.get("country"),
        "department": cats.get("department") or cats.get("team"),
        "job_url": job.get("hostedUrl") or job.get("applyUrl"),
        "description": job.get("descriptionPlain") or strip_html(job.get("description")),
        "is_remote": bool(
            str(cats.get("commitment") or "").lower().find("remote") >= 0
            or str(job.get("workplaceType") or "").lower() == "remote"
        ),
        "min_amount": None,
        "max_amount": None,
        "currency": None,
        "published_at": job.get("createdAt") and str(job.get("createdAt")),
        "scraped_at": datetime.now(timezone.utc),
        "organization_id": None,
        "board_slug": lab["slug"],
    }


def setup(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS jobspy")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS jobspy.ats_jobs (
            id VARCHAR,
            site VARCHAR,
            company VARCHAR,
            domain VARCHAR,
            organization_id INTEGER,
            board_slug VARCHAR,
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
    ensure_contract_columns(con, "ats_jobs", backfill=True)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS jobspy.ats_board_runs (
            ats VARCHAR,
            slug VARCHAR,
            started_at TIMESTAMP WITH TIME ZONE,
            finished_at TIMESTAMP WITH TIME ZONE,
            rows_found INTEGER,
            error VARCHAR,
            PRIMARY KEY (ats, slug)
        )
        """
    )


def load_boards(con) -> list[dict]:
    rows = con.execute(
        """
        SELECT ats, slug, coalesce(company, slug) AS company, domain
        FROM ats_raw.boards
        WHERE ats IN ('greenhouse', 'ashby', 'lever')
          AND slug IS NOT NULL
        """
    ).fetchall()
    return [
        {"ats": ats, "slug": slug, "company": company, "domain": domain or ""}
        for ats, slug, company, domain in rows
    ]


def pull_board(lab: dict) -> pd.DataFrame:
    ats = lab["ats"]
    if ats == "greenhouse":
        rows = [normalize_greenhouse(lab, job) for job in greenhouse_jobs(lab["slug"])]
    elif ats == "ashby":
        rows = [normalize_ashby(lab, job) for job in ashby_jobs(lab["slug"])]
    elif ats == "lever":
        rows = [normalize_lever(lab, job) for job in lever_jobs(lab["slug"])]
    else:
        rows = []
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame["board_slug"] = lab["slug"]
        if "organization_id" not in frame.columns:
            frame["organization_id"] = None
    return frame


def safe_print(line: str) -> None:
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        print(line.encode("ascii", "replace").decode("ascii"), flush=True)


def main() -> int:
    safe_print(
        "Stopped: JobSpy no longer collects Greenhouse, Ashby, or Lever boards. "
        "ats_raw already covers them."
    )
    return 0
    con = connect("lighthouse")
    setup(con)
    ids = org_ids(con)
    done = {
        (row[0], row[1])
        for row in con.execute(
            """
            SELECT ats, slug FROM jobspy.ats_board_runs
            WHERE finished_at IS NOT NULL AND error IS NULL
            """
        ).fetchall()
    }
    boards = [board for board in load_boards(con) if (board["ats"], board["slug"]) not in done]
    safe_print(f"Fetching {len(boards)} ATS boards (skip {len(done)} already done)")
    write_lock = __import__("threading").Lock()

    def run(board: dict) -> tuple[str, str, int, str | None]:
        with write_lock:
            con.execute(
                """
                INSERT OR REPLACE INTO jobspy.ats_board_runs
                    (ats, slug, started_at, finished_at, rows_found, error)
                VALUES (?, ?, ?, NULL, NULL, NULL)
                """,
                [board["ats"], board["slug"], datetime.now(timezone.utc)],
            )
        try:
            frame = pull_board(board)
            if not frame.empty:
                domain = (board.get("domain") or "").lower()
                frame["organization_id"] = ids.get(domain)
                with write_lock:
                    upsert_by_id(con, "ats_jobs", frame)
            found = 0 if frame is None or frame.empty else len(frame)
            with write_lock:
                con.execute(
                    """
                    UPDATE jobspy.ats_board_runs
                    SET finished_at = ?, rows_found = ?, error = NULL
                    WHERE ats = ? AND slug = ?
                    """,
                    [datetime.now(timezone.utc), found, board["ats"], board["slug"]],
                )
            return board["company"], board["ats"], found, None
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            with write_lock:
                con.execute(
                    """
                    UPDATE jobspy.ats_board_runs
                    SET finished_at = ?, rows_found = 0, error = ?
                    WHERE ats = ? AND slug = ?
                    """,
                    [datetime.now(timezone.utc), msg, board["ats"], board["slug"]],
                )
            return board["company"], board["ats"], 0, msg

    completed = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(run, board) for board in boards]
        for future in as_completed(futures):
            company, ats, found, error = future.result()
            completed += 1
            if error:
                safe_print(f"[{completed}/{len(boards)}] {ats}/{company}: ERROR {error}")
            else:
                safe_print(f"[{completed}/{len(boards)}] {ats}/{company}: {found} jobs")
            if completed % 25 == 0:
                time.sleep(0.2)

    totals = con.execute(
        """
        SELECT site, COUNT(*) jobs, COUNT(DISTINCT company) companies
        FROM jobspy.ats_jobs
        GROUP BY 1
        ORDER BY jobs DESC
        """
    ).fetchall()
    grand = con.execute("SELECT COUNT(*) FROM jobspy.ats_jobs").fetchone()[0]
    safe_print("Done. jobspy.ats_jobs")
    for site, jobs, companies in totals:
        safe_print(f"  {site}: {jobs} jobs / {companies} companies")
    safe_print(f"Total {grand}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
