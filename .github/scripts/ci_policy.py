"""Select CI coverage and enforce its final result (Python 3.9+)."""

import json
import os
import subprocess
import sys
from pathlib import Path, PurePosixPath

PYTHONS = ["3.9", "3.x"]
CDA = {
    "production": "ghcr.io/usace/cwms-data-api:2026.05.12-i",
    "test": "ghcr.io/usace/cwms-data-api:2026.08.31-testd",
    "latest": "ghcr.io/usace/cwms-data-api:develop-nightly",
}
SCHEMAS = {"production": "26.02.17", "test": "26.07.16-RC02", "latest": "latest-dev"}


def documentation(path):
    p = PurePosixPath(path)
    return (len(p.parts) == 1 and p.suffix.lower() == ".md") or (
        p.parts[0] in {"docs", "rtd_docs"}
        and p.suffix.lower()
        in {".md", ".rst", ".txt", ".png", ".jpg", ".jpeg", ".svg", ".gif"}
    )


def plan(event_name, event, paths=None):
    if event_name == "push":
        mode = "main"
    elif event_name == "schedule":
        mode = "full"
    elif event_name == "workflow_dispatch":
        mode = event.get("inputs", {}).get("coverage", "full")
        if mode not in {"full", "representative"}:
            raise ValueError("Unknown coverage mode")
    elif event_name == "pull_request":
        if event["pull_request"]["draft"]:
            mode = "draft"
        elif paths and all(documentation(p) for p in paths):
            mode = "documentation"
        else:
            mode = "representative"
    else:
        raise ValueError("Unknown event")
    rows = [
        {"python": python, "cda": cda, "image": image, "schema": schema, "tag": tag}
        for python in PYTHONS
        for cda, image in CDA.items()
        for schema, tag in SCHEMAS.items()
        if mode == "full" or (mode == "representative" and cda == schema)
    ]
    return {
        "mode": mode,
        "integration": str(bool(rows)).lower(),
        "matrix": {"include": rows},
    }


def gate(needs):
    for name in ("plan", "format", "unit"):
        if needs[name]["result"] != "success":
            raise ValueError(f"{name} did not pass")
    mode = needs["plan"]["outputs"]["mode"]
    result = needs["integration"]["result"]
    if mode in {"full", "representative"}:
        if result != "success":
            raise ValueError("Required integration tests did not pass")
    elif mode in {"main", "draft", "documentation"}:
        if result != "skipped":
            raise ValueError("Unexpected integration result")
    else:
        raise ValueError("Missing or unknown CI mode")


def main():
    if sys.argv[1] == "gate":
        gate(json.loads(os.environ["CI_NEEDS"]))
        print("All applicable CI checks passed")
        return
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    event_name = os.environ["GITHUB_EVENT_NAME"]
    paths = None
    if event_name == "pull_request":
        pr = event["pull_request"]
        try:
            diff = subprocess.check_output(
                [
                    "git",
                    "diff",
                    "--name-only",
                    "--no-renames",
                    "-z",
                    pr["base"]["sha"] + "..." + pr["head"]["sha"],
                    "--",
                ]
            )
            paths = [p for p in diff.decode("utf-8").split("\0") if p]
        except (subprocess.CalledProcessError, UnicodeDecodeError):
            print("Could not classify changed files; requiring integration tests")
    result = plan(event_name, event, paths)
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        for key, value in result.items():
            output.write(
                f"{key}={json.dumps(value) if isinstance(value, dict) else value}\n"
            )
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
        summary.write(
            f"CI coverage: **{result['mode']}**; CDA jobs: {len(result['matrix']['include'])}.\n"
        )


if __name__ == "__main__":
    main()
