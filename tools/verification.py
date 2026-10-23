from __future__ import annotations

import argparse
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import shutil
import sys

from tools.verification_environment import clean, doctor
from tools.verification_evidence import Status, Summary, publish, report, save
from tools.verification_lanes import backend_lane, execute, focused, fresh
from tools.verification_process import run
from tools.verification_services import BACKENDS, configured


def aggregate(lane: str, outcomes: list[Summary], full: bool = False) -> Summary:
    status: Status = "PASS"
    if any(item.status == "FAIL" for item in outcomes):
        status = "FAIL"
    elif not outcomes or any(item.status != "PASS" for item in outcomes):
        status = "INCOMPLETE"
    directory = Path("test-results") / lane
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "lanes.json").write_text(json.dumps([asdict(item) for item in outcomes], indent=2))
    summary = Summary(
        1,
        lane,
        ("make", lane),
        sum(s.selected for s in outcomes),
        sum(s.passed for s in outcomes),
        sum(s.failed for s in outcomes),
        sum(s.skipped for s in outcomes),
        status,
        int(status != "PASS"),
        scope="FULL_LOCAL" if full else "PARTIAL",
        reason="; ".join(f"{s.lane}={s.status}" for s in outcomes),
    )
    summary = save(directory, summary)
    publish(summary)
    return summary


def integration(external: bool = False) -> int:
    directory = Path("test-results/integration")
    fresh(directory)
    selection = os.environ.get("DB_BACKEND", "")
    if external and os.environ.get("DB_TEST_DATABASE_CONFIRMED") != "1":
        summary = Summary(
            1,
            "integration",
            ("make", "integration-local"),
            0,
            0,
            0,
            0,
            "INCOMPLETE",
            2,
            reason="Set DB_TEST_DATABASE_CONFIRMED=1 only for a dedicated test database; "
            "fixtures create/drop test tables",
        )
        publish(save(directory, summary))
        return 2
    if selection and selection not in BACKENDS:
        summary = Summary(
            1,
            "integration",
            ("make", "integration"),
            0,
            0,
            0,
            0,
            "INCOMPLETE",
            2,
            reason="Unsupported DB_BACKEND",
        )
        publish(save(directory, summary))
        return 2
    outcomes = [
        backend_lane(backend, owned=not external)
        for backend in ((selection,) if selection else BACKENDS)
    ]
    for backend in (selection,) if selection else BACKENDS:
        source = directory / backend
        for pattern, destination in (("*junit.xml", "junit"), ("logs/*", "logs")):
            for artifact in source.glob(pattern):
                target = directory / destination / f"{backend}-{artifact.name}"
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(artifact, target)
    return aggregate("integration", outcomes).exit_code


def verify() -> int:
    directory = Path("test-results/verify")
    fresh(directory)
    outcomes = []
    with configured("DB_BACKEND", ""):
        for target in (
            "check",
            "test",
            "integration",
            "host-smoke",
            "wheel-smoke",
            "build",
            "docs",
        ):
            path = Path("test-results") / target / "summary.json"
            path.unlink(missing_ok=True)
            code = run(["make", target, "DB_BACKEND="], directory / "logs" / f"{target}.log", 1800)
            if path.exists():
                summary = Summary(**json.loads(path.read_text()))
                if code and summary.status == "PASS":
                    summary = replace(summary, status="FAIL", exit_code=code)
            else:
                summary = Summary(
                    1,
                    target,
                    ("make", target),
                    int(target == "check"),
                    int(target == "check" and code == 0),
                    int(code != 0),
                    0,
                    ("FAIL" if code else "PASS") if target == "check" else "INCOMPLETE",
                    code or int(target != "check"),
                    reason="Required lane summary missing",
                )
            outcomes.append(summary)
    return aggregate("verify", outcomes, full=True).exit_code


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "lane",
        choices=(
            "test",
            "backend",
            "integration",
            "integration-local",
            "verify",
            "azure-e2e",
            "report",
            "doctor",
            "clean",
            "focused",
        ),
    )
    args, extra = parser.parse_known_args()
    if not (3, 11) <= sys.version_info[:2] < (3, 15):
        parser.error("Python >=3.11,<3.15 required")
    match args.lane:
        case "doctor":
            return doctor()
        case "report":
            return report(Path("test-results"))
        case "clean":
            logical = Path(os.environ.get("PWD", str(Path.cwd())))
            root = logical if logical.resolve() == Path.cwd() else Path.cwd()
            try:
                clean(root)
            except ValueError as error:
                print(str(error), file=sys.stderr)
                return 1
            return 0
        case "verify":
            return verify()
        case "integration" | "integration-local":
            return integration(external=args.lane == "integration-local")
        case "backend":
            if not extra or extra[0] not in BACKENDS:
                parser.error("backend requires " + ", ".join(BACKENDS))
            return backend_lane(
                extra[0],
                owned="--owned" in extra,
                extra=tuple(arg for arg in extra[1:] if arg != "--owned"),
            ).exit_code
        case "focused":
            return focused(extra[0], [sys.executable, *extra[1:]]).exit_code
        case _:
            return execute(args.lane, extra).exit_code


if __name__ == "__main__":
    raise SystemExit(main())
