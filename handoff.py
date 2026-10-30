#!/usr/bin/env python3
"""
handoff.py WORKFLOW_FILE — called by a recording run near its end (both recorders' workflows, via --handoff-cmd).

Starts the next run of WORKFLOW_FILE, unless a NEWER run of the same workflow is already recording or queued: that
run's chain already covers the time after this run ends, and starting another one would keep two chains running side
by side (seen 2026-10-30: the hourly backup timer started a second Bitkub chain). So duplicate chains shrink back to
one at their next hand-over. A newer run counts only after 90 s (a backup-timer run that finds a recording run ends
within ~10 s doing nothing). If the check itself fails, the next run is started anyway (never a hole).
Needs GH_TOKEN, GITHUB_REPOSITORY, GITHUB_RUN_ID, GITHUB_REF_NAME (set by GitHub Actions). Prints one result line.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime
from typing import Any, Callable, Dict, List

MIN_AGE_S = 90


def newer_runs(runs: List[Dict[str, Any]], me: int, now: float) -> List[int]:
    """Ids of runs started after this one (GitHub run ids increase), not finished, older than MIN_AGE_S."""
    return [r["databaseId"] for r in runs
            if r["databaseId"] > me and r["status"] != "completed"
            and now - datetime.fromisoformat(r["createdAt"].replace("Z", "+00:00")).timestamp() > MIN_AGE_S]


def main(wf: str, run: Callable[..., Any] = subprocess.run, now: Callable[[], float] = time.time,
         env: Dict[str, str] = os.environ) -> int:  # type: ignore[assignment]
    repo, me = env["GITHUB_REPOSITORY"], int(env["GITHUB_RUN_ID"])
    try:
        out = run(["gh", "run", "list", "--repo", repo, "--workflow", wf, "--limit", "20",
                   "--json", "databaseId,status,createdAt"], capture_output=True, text=True, timeout=60, check=True)
        newer = newer_runs(json.loads(out.stdout), me, now())
    except Exception:  # noqa: BLE001 - the check is only an optimisation: hand over as before
        newer = []
    if newer:
        print(f"skipped: newer run {','.join(map(str, newer))} is already recording")
        return 0
    r = run(["gh", "workflow", "run", wf, "--repo", repo, "--ref", env["GITHUB_REF_NAME"]],
            capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        print((r.stderr or r.stdout or "gh workflow run failed").strip()[:150])
        return r.returncode
    print("started next run")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
