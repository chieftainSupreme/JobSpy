"""Unattended supervisor: keep resume-safe collectors running."""

from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

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


def spawn(name: str, argv: list[str]) -> None:
    log_path = LOG_DIR / f"{name}.log"
    handle = log_path.open("a", encoding="utf-8")
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    subprocess.Popen(
        argv,
        cwd=str(ROOT),
        stdout=handle,
        stderr=subprocess.STDOUT,
        env=env,
        creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
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


def loop() -> int:
    log("supervisor starting")
    cycle_path = LOG_DIR / "indeed_cycle"
    if pids_matching("collect_data_jobs.py") and not cycle_path.exists():
        cycle_path.write_text("1", encoding="utf-8")
        log("wave1 already running; next Indeed start will be extra countries wave 2")
    while True:
        ensure("workday", "collect_workday.py", python_job("collect_workday.py"), 21600)
        ensure(
            "more_sites",
            "collect_more_sites.py",
            python_job("collect_more_sites.py"),
            120,
        )
        ensure("ats", "collect_ats_boards.py", python_job("collect_ats_boards.py"), 1800)
        ensure(
            "other_ats",
            "collect_other_ats.py",
            python_job("collect_other_ats.py"),
            1800,
        )
        indeed_cycle()
        time.sleep(20)
    return 0


if __name__ == "__main__":
    raise SystemExit(loop())
