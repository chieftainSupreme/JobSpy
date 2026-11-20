"""Unattended supervisor: keep resume-safe collectors running."""

from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from collect_more_sites import HIGH_YIELD

ROOT = Path(__file__).resolve().parent
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

WAVE1 = [
    "Australia",
    "Ireland",
    "Germany",
    "Netherlands",
    "India",
    "Singapore",
    "France",
]
WAVE2 = [
    "Mexico",
    "Brazil",
    "Japan",
    "Spain",
    "Italy",
    "Israel",
    "South Africa",
    "New Zealand",
    "Belgium",
    "Poland",
    "Switzerland",
    "Sweden",
    "UAE",
    "Hong Kong",
    "South Korea",
    "Philippines",
    "Portugal",
    "Denmark",
]
WAVE3 = [
    "Norway",
    "Finland",
    "Austria",
    "Czech Republic",
    "Greece",
    "Romania",
    "Malaysia",
    "Indonesia",
    "Taiwan",
    "Thailand",
    "Argentina",
    "Chile",
    "Colombia",
    "Saudi Arabia",
    "Nigeria",
    "Vietnam",
]
WAVE4 = [
    "Egypt",
    "Hungary",
    "Luxembourg",
    "Morocco",
    "Pakistan",
    "Peru",
    "Qatar",
    "Turkey",
    "Ukraine",
    "China",
    "Costa Rica",
    "Ecuador",
    "Panama",
    "Uruguay",
]

# Data and GTM country grids are finished. Indeed capacity stays on data titles.

GTM_TERM_BATCHES = [
    [
        "account executive",
        "sales development representative",
        "business development representative",
        "SDR",
        "BDR",
        "account manager",
        "enterprise account executive",
        "sales manager",
        "sales engineer",
        "solutions consultant",
    ],
    [
        "solutions engineer",
        "customer success manager",
        "customer success",
        "implementation manager",
        "onboarding specialist",
        "technical account manager",
        "client partner",
        "customer support",
        "support engineer",
        "customer experience",
    ],
    [
        "product marketing",
        "demand generation",
        "growth marketing",
        "content marketing",
        "field marketing",
        "lifecycle marketing",
        "marketing manager",
        "brand manager",
        "digital marketing",
        "performance marketing",
    ],
    [
        "sales operations",
        "marketing operations",
        "partner manager",
        "channel manager",
        "sales enablement",
        "business development",
        "SDR manager",
        "head of sales",
        "VP of sales",
        "revenue operations manager",
    ],
    [
        "account executive intern",
        "sales intern",
        "marketing intern",
        "customer success intern",
        "partnerships manager",
        "alliances manager",
        "solutions architect",
        "pre-sales engineer",
        "inside sales",
        "field sales",
    ],
]

# Older data-only extra batches; kept for resume if FOCUS returns to data.
EXTRA_TERM_BATCHES = [
    [
        "geospatial analyst",
        "GIS analyst",
        "bioinformatics",
        "computational biologist",
        "clinical data manager",
        "epidemiologist",
        "HEOR analyst",
        "model risk",
        "LLMOps",
        "AI platform engineer",
    ],
    [
        "inference engineer",
        "model serving",
        "microsoft fabric",
        "data factory",
        "glue engineer",
        "unity catalog",
        "delta lake",
        "iceberg",
        "great expectations",
        "reverse ETL",
    ],
    [
        "personalization",
        "relevance engineer",
        "recsys",
        "forward deployed engineer",
        "analytics translator",
        "staff analytics engineer",
        "principal analytics engineer",
        "head of data engineering",
        "head of data science",
        "director of ML",
    ],
    [
        "reinforcement learning",
        "robotics perception",
        "sensor fusion",
        "IoT analytics",
        "digital twin",
        "adobe analytics",
        "GA4 analyst",
        "vertex AI",
        "sagemaker",
        "azure ml",
    ],
    [
        "staff data analyst",
        "lead data scientist",
        "lead data engineer",
        "junior data analyst",
        "associate data scientist",
        "VP of data",
        "VP of analytics",
        "chief analytics officer",
        "data contracts",
        "metrics engineer",
    ],
]


def log(line: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    text = f"{stamp} {line}"
    print(text, flush=True)
    with (LOG_DIR / "keep_collecting.log").open("a", encoding="utf-8") as handle:
        handle.write(text + "\n")


def pids_matching(needle: str) -> list[int]:
    try:
        out = subprocess.check_output(
            [
                "powershell",
                "-NoProfile",
                "-File",
                str(ROOT / "list_python_jobs.ps1"),
            ],
            text=True,
            errors="replace",
            stderr=subprocess.DEVNULL,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []
    text = out.strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return []
    if isinstance(payload, dict):
        payload = [payload]
    needle_l = needle.lower()
    pids: list[int] = []
    for row in payload:
        command = str(row.get("CommandLine") or "")
        pid = row.get("ProcessId")
        if (
            pid
            and needle_l in command.lower()
            and "keep_collecting.py" not in command.lower()
        ):
            pids.append(int(pid))
    return pids


def last_start(name: str) -> float:
    path = LOG_DIR / f"{name}.last_start"
    try:
        return path.stat().st_mtime
    except FileNotFoundError:
        return 0.0


CREATE_NEW_PROCESS_GROUP = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
CREATE_NO_WINDOW = 0x08000000
CREATE_BREAKAWAY_FROM_JOB = 0x01000000


def spawn(name: str, argv: list[str]) -> None:
    log_path = LOG_DIR / f"{name}.log"
    handle = log_path.open("a", encoding="utf-8")
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    flags = CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW | CREATE_BREAKAWAY_FROM_JOB
    try:
        subprocess.Popen(
            argv,
            cwd=str(ROOT),
            stdout=handle,
            stderr=subprocess.STDOUT,
            env=env,
            close_fds=False,
            creationflags=flags,
        )
    except OSError:
        subprocess.Popen(
            argv,
            cwd=str(ROOT),
            stdout=handle,
            stderr=subprocess.STDOUT,
            env=env,
            close_fds=False,
            creationflags=CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW,
        )
    (LOG_DIR / f"{name}.last_start").write_text(str(time.time()), encoding="utf-8")
    log(f"started {name}: {' '.join(Path(a).name if '\\\\' in a or '/' in a else a for a in argv[1:])}")


def python_job(script: str, extra: list[str] | None = None) -> list[str]:
    argv = [str(PYTHON), str(ROOT / script)]
    if extra:
        argv.extend(extra)
    return argv


def ensure(name: str, needle: str, argv: list[str], cooldown_s: int) -> None:
    if pids_matching(needle):
        return
    if time.time() - last_start(name) < cooldown_s:
        return
    spawn(name, argv)


def extra_batch_index() -> int:
    path = LOG_DIR / "indeed_extra_cycle"
    try:
        return int(path.read_text(encoding="utf-8").strip() or "0")
    except (FileNotFoundError, ValueError):
        return 0


def gtm_batch_index() -> int:
    path = LOG_DIR / "indeed_gtm_cycle"
    try:
        return int(path.read_text(encoding="utf-8").strip() or "0")
    except (FileNotFoundError, ValueError):
        return 0


def indeed_cycle() -> None:
    if pids_matching("collect_data_jobs.py"):
        return
    path = LOG_DIR / "indeed_cycle"
    try:
        cycle = int(path.read_text(encoding="utf-8").strip() or "0")
    except (FileNotFoundError, ValueError):
        cycle = 0
    if cycle <= 0:
        spawn("indeed_wave1", python_job("collect_data_jobs.py", ["--countries", *WAVE1]))
        path.write_text("1", encoding="utf-8")
    elif cycle == 1:
        spawn("indeed_wave2", python_job("collect_data_jobs.py", ["--countries", *WAVE2]))
        path.write_text("2", encoding="utf-8")
    elif cycle == 2:
        spawn("indeed_wave3", python_job("collect_data_jobs.py", ["--countries", *WAVE3]))
        path.write_text("3", encoding="utf-8")
    elif cycle == 3:
        spawn("indeed_wave4", python_job("collect_data_jobs.py", ["--countries", *WAVE4]))
        path.write_text("4", encoding="utf-8")
    else:
        gtm_path = LOG_DIR / "indeed_gtm_cycle"
        idx = gtm_batch_index()
        if idx < len(GTM_TERM_BATCHES):
            spawn(
                "indeed",
                python_job(
                    "collect_data_jobs.py",
                    ["--terms", *GTM_TERM_BATCHES[idx]],
                ),
            )
            gtm_path.write_text(str(idx + 1), encoding="utf-8")
            return
        extra_path = LOG_DIR / "indeed_extra_cycle"
        extra_idx = extra_batch_index()
        if extra_idx < len(EXTRA_TERM_BATCHES):
            spawn(
                "indeed",
                python_job(
                    "collect_data_jobs.py",
                    ["--terms", *EXTRA_TERM_BATCHES[extra_idx]],
                ),
            )
            extra_path.write_text(str(extra_idx + 1), encoding="utf-8")
            return
        ensure(
            "indeed",
            "collect_data_jobs.py",
            python_job("collect_data_jobs.py", ["--terms", *HIGH_YIELD]),
            1800,
        )


def loop() -> int:
    log("supervisor starting")
    cycle_path = LOG_DIR / "indeed_cycle"
    if pids_matching("collect_data_jobs.py") and not cycle_path.exists():
        cycle_path.write_text("1", encoding="utf-8")
        log("wave1 already running; next Indeed start will be extra countries wave 2")
    while True:
        # ATS collectors stay stopped: they duplicate ats_raw and rarely store descriptions.
        ensure(
            "more_sites",
            "collect_more_sites.py",
            python_job("collect_more_sites.py"),
            1800,
        )
        ensure(
            "city_splits",
            "collect_city_splits.py",
            python_job("collect_city_splits.py"),
            900,
        )
        indeed_cycle()
        time.sleep(20)
    return 0


if __name__ == "__main__":
    raise SystemExit(loop())
