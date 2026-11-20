"""Fetch Workable and Recruitee boards from ats_raw.boards into jobspy.ats_jobs."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import json

import pandas as pd

from collect_ats_boards import setup, strip_html
from collect_data_jobs import connect, upsert_by_id
from collect_frontier_labs import org_ids


def safe_print(line: str) -> None:
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        print(line.encode("ascii", "replace").decode("ascii"), flush=True)


def fetch_json(url: str):
    request = Request(url, headers={"User-Agent": "Mozilla/5.0 JobSpy other-ats"})
    with urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def workable_jobs(slug: str) -> list[dict]:
    url = f"https://apply.workable.com/api/v1/widget/accounts/{slug}"
    payload = fetch_json(url)
    return list(payload.get("jobs") or [])


def recruitee_jobs(slug: str) -> list[dict]:
    url = f"https://{slug}.recruitee.com/api/offers/"
    payload = fetch_json(url)
    return list(payload.get("offers") or [])


def normalize_workable(board: dict, job: dict) -> dict:
    location = job.get("location") or job.get("city")
    if isinstance(location, dict):
        location = ", ".join(
            str(part)
            for part in [location.get("city"), location.get("country")]
            if part
        )
    return {
        "id": f"workable-{board['slug']}-{job.get('id') or job.get('shortcode') or job.get('title')}",
        "site": "workable",
        "company": board["company"],
        "domain": board["domain"],
        "organization_id": None,
        "board_slug": board["slug"],
        "title": job.get("title"),
        "location": location,
        "department": job.get("department") or job.get("function"),
        "job_url": job.get("url") or job.get("application_url"),
        "description": strip_html(job.get("description")),
        "is_remote": bool(job.get("remote") or (location and "remote" in str(location).lower())),
        "min_amount": None,
        "max_amount": None,
        "currency": None,
        "published_at": str(job.get("published_on") or job.get("created_at") or "") or None,
        "scraped_at": datetime.now(timezone.utc),
    }


def normalize_recruitee(board: dict, job: dict) -> dict:
    loc = job.get("location") or {}
    if isinstance(loc, dict):
        location = ", ".join(
            str(part) for part in [loc.get("city"), loc.get("country")] if part
        )
    else:
        location = loc
    return {
        "id": f"recruitee-{board['slug']}-{job.get('id') or job.get('slug') or job.get('title')}",
        "site": "recruitee",
        "company": board["company"],
        "domain": board["domain"],
        "organization_id": None,
        "board_slug": board["slug"],
        "title": job.get("title"),
        "location": location or job.get("location_slug"),
        "department": job.get("department") or job.get("category_code"),
        "job_url": job.get("careers_url") or job.get("url"),
        "description": job.get("description_html") and strip_html(job.get("description_html"))
        or job.get("description"),
        "is_remote": bool(job.get("remote") or job.get("on_site") is False),
        "min_amount": None,
        "max_amount": None,
        "currency": None,
        "published_at": str(job.get("published_at") or job.get("created_at") or "") or None,
        "scraped_at": datetime.now(timezone.utc),
    }


def pull_board(board: dict) -> pd.DataFrame:
    if board["ats"] == "workable":
        rows = [normalize_workable(board, job) for job in workable_jobs(board["slug"])]
    else:
        rows = [normalize_recruitee(board, job) for job in recruitee_jobs(board["slug"])]
    return pd.DataFrame(rows)


def main() -> int:
    safe_print(
        "Stopped: JobSpy no longer collects Workable or Recruitee boards. "
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
    boards = [
        {"ats": ats, "slug": slug, "company": company, "domain": domain or ""}
        for ats, slug, company, domain in con.execute(
            """
            SELECT ats, slug, coalesce(company, slug), domain
            FROM ats_raw.boards
            WHERE ats IN ('workable', 'recruitee') AND slug IS NOT NULL
            """
        ).fetchall()
        if (ats, slug) not in done
    ]
    safe_print(f"Fetching {len(boards)} Workable/Recruitee boards (skip {len(done)} done)")
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
            found = 0 if frame.empty else len(frame)
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
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(run, board) for board in boards]
        for future in as_completed(futures):
            company, ats, found, error = future.result()
            completed += 1
            if error:
                safe_print(f"[{completed}/{len(boards)}] {ats}/{company}: ERROR {error}")
            else:
                safe_print(f"[{completed}/{len(boards)}] {ats}/{company}: {found} jobs")

    totals = con.execute(
        """
        SELECT site, COUNT(*) jobs
        FROM jobspy.ats_jobs
        GROUP BY 1
        ORDER BY jobs DESC
        """
    ).fetchall()
    safe_print("Done. jobspy.ats_jobs")
    for site, jobs in totals:
        safe_print(f"  {site}: {jobs}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
