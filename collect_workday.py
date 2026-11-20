"""Fetch every job from Workday boards in ats_raw.boards.

Existing open rows in ats_raw.workday_postings are copied first so
jobspy.ats_jobs has the catalog immediately, then each board is paged
from the public CXS API until it returns no more jobs.
"""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html import unescape
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd

from collect_ats_boards import setup, strip_html
from collect_data_jobs import connect, upsert_by_id
from collect_frontier_labs import org_ids

PAGE_SIZE = 20
WORKERS = 3


def safe_print(line: str) -> None:
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        print(line.encode("ascii", "replace").decode("ascii"), flush=True)


def workday_url(slug: str) -> str:
    parts = [part for part in slug.split("/") if part]
    if len(parts) < 3:
        raise ValueError(f"unexpected workday slug: {slug}")
    tenant, instance, site = parts[0], parts[1], "/".join(parts[2:])
    return f"https://{tenant}.{instance}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs"


def fetch_page(url: str, offset: int) -> dict:
    body = json.dumps(
        {"appliedFacets": {}, "limit": PAGE_SIZE, "offset": offset, "searchText": ""}
    ).encode("utf-8")
    request = Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 JobSpy workday",
        },
        method="POST",
    )
    with urlopen(request, timeout=90) as response:
        return json.loads(response.read().decode("utf-8"))


def workday_jobs(slug: str) -> list[dict]:
    url = workday_url(slug)
    offset = 0
    rows: list[dict] = []
    total = None
    while True:
        payload = fetch_page(url, offset)
        if total is None:
            total = int(payload.get("total") or 0)
        postings = list(payload.get("jobPostings") or [])
        if not postings:
            break
        rows.extend(postings)
        offset += len(postings)
        if total and offset >= total:
            break
        if len(postings) < PAGE_SIZE:
            break
        time.sleep(0.15)
    return rows


def normalize_workday(board: dict, job: dict) -> dict:
    info = job.get("jobPostingInfo") or job
    title = info.get("title") or job.get("title")
    location = (
        info.get("location")
        or job.get("locationsText")
        or job.get("bulletFields")
        and ", ".join(str(x) for x in job.get("bulletFields") or [])
    )
    if isinstance(location, dict):
        location = location.get("descriptor") or location.get("name")
    country = info.get("country")
    if isinstance(country, dict):
        country = country.get("descriptor")
    if country and location and str(country) not in str(location):
        location = f"{location}, {country}"
    elif not location:
        location = country
    job_id = (
        info.get("id")
        or job.get("jobPostingId")
        or job.get("bulletFields")
        or job.get("externalPath")
        or title
    )
    path = job.get("externalPath") or ""
    job_url = info.get("externalUrl")
    if not job_url and path:
        parts = [p for p in board["slug"].split("/") if p]
        tenant, instance, site = parts[0], parts[1], "/".join(parts[2:])
        if path.startswith("/"):
            job_url = f"https://{tenant}.{instance}.myworkdayjobs.com{path}"
        else:
            job_url = f"https://{tenant}.{instance}.myworkdayjobs.com/{site}{path}"
    return {
        "id": f"workday-{board['slug']}-{job_id}",
        "site": "workday",
        "company": board["company"],
        "domain": board["domain"],
        "organization_id": None,
        "board_slug": board["slug"],
        "title": title,
        "location": location,
        "department": (job.get("jobFamilyGroup") or info.get("jobFamily") or None),
        "job_url": job_url,
        "description": strip_html(info.get("jobDescription")),
        "is_remote": bool(location and "remote" in str(location).lower()),
        "min_amount": None,
        "max_amount": None,
        "currency": None,
        "published_at": str(info.get("startDate") or job.get("postedOn") or "") or None,
        "scraped_at": datetime.now(timezone.utc),
    }


def copy_existing(con) -> int:
    """Copying ats_raw.workday_postings into jobspy.ats_jobs is stopped."""
    del con
    raise RuntimeError(
        "Copying ats_raw.workday_postings into jobspy.ats_jobs is stopped"
    )


def load_boards(con) -> list[dict]:
    rows = con.execute(
        """
        SELECT slug, coalesce(company, slug) AS company, domain
        FROM ats_raw.boards
        WHERE ats = 'workday' AND slug IS NOT NULL
        """
    ).fetchall()
    return [
        {"ats": "workday", "slug": slug, "company": company, "domain": domain or ""}
        for slug, company, domain in rows
    ]


def upsert_frame(con, frame: pd.DataFrame, write_lock) -> int:
    if frame.empty:
        return 0
    with write_lock:
        return upsert_by_id(con, "ats_jobs", frame)


def main() -> int:
    safe_print(
        "Stopped: JobSpy no longer collects Workday boards or copies "
        "ats_raw.workday_postings into jobspy.ats_jobs."
    )
    return 0
    con = connect("lighthouse")
    setup(con)
    ids = org_ids(con)
    done = {
        row[0]
        for row in con.execute(
            """
            SELECT slug FROM jobspy.ats_board_runs
            WHERE ats = 'workday' AND finished_at IS NOT NULL AND error IS NULL
            """
        ).fetchall()
    }
    boards = [board for board in load_boards(con) if board["slug"] not in done]
    safe_print(f"Live-fetching {len(boards)} Workday boards (skip {len(done)})")
    write_lock = __import__("threading").Lock()

    def run(board: dict) -> tuple[str, int, str | None]:
        with write_lock:
            con.execute(
                """
                INSERT OR REPLACE INTO jobspy.ats_board_runs
                    (ats, slug, started_at, finished_at, rows_found, error)
                VALUES ('workday', ?, ?, NULL, NULL, NULL)
                """,
                [board["slug"], datetime.now(timezone.utc)],
            )
        try:
            rows = [normalize_workday(board, job) for job in workday_jobs(board["slug"])]
            frame = pd.DataFrame(rows)
            if not frame.empty:
                domain = (board.get("domain") or "").lower()
                frame["organization_id"] = ids.get(domain)
            found = upsert_frame(con, frame, write_lock)
            with write_lock:
                con.execute(
                    """
                    UPDATE jobspy.ats_board_runs
                    SET finished_at = ?, rows_found = ?, error = NULL
                    WHERE ats = 'workday' AND slug = ?
                    """,
                    [datetime.now(timezone.utc), found, board["slug"]],
                )
            return board["company"], found, None
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            with write_lock:
                con.execute(
                    """
                    UPDATE jobspy.ats_board_runs
                    SET finished_at = ?, rows_found = 0, error = ?
                    WHERE ats = 'workday' AND slug = ?
                    """,
                    [datetime.now(timezone.utc), msg, board["slug"]],
                )
            return board["company"], 0, msg

    completed = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(run, board) for board in boards]
        for future in as_completed(futures):
            company, found, error = future.result()
            completed += 1
            if error:
                safe_print(f"[{completed}/{len(boards)}] workday/{company}: ERROR {error}")
            else:
                safe_print(f"[{completed}/{len(boards)}] workday/{company}: {found} jobs")

    total = con.execute(
        "SELECT COUNT(*) FROM jobspy.ats_jobs WHERE site = 'workday'"
    ).fetchone()[0]
    grand = con.execute("SELECT COUNT(*) FROM jobspy.ats_jobs").fetchone()[0]
    safe_print(f"Done. workday={total} ats_jobs={grand}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
