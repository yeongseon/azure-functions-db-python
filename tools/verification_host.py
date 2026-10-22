from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import time
import xml.etree.ElementTree as ET

from tools.verification_process import child
from tools.verification_services import (
    MissingPrerequisite,
    ServiceFailure,
    azurite,
    configured,
    port,
)


def main() -> int:
    directory = Path("test-results/host-smoke")
    if shutil.which("func") is None:
        raise MissingPrerequisite("func required for host-smoke/verify")
    with tempfile.TemporaryDirectory(prefix="db-host-") as temporary:
        app = Path(temporary)
        for filename in ("function_app.py", "host.json"):
            shutil.copyfile(Path("tests/host_smoke") / filename, app / filename)
        source = str(Path("src").resolve())
        fixture = app / "function_app.py"
        fixture.write_text(
            fixture.read_text()
            + "\nimport azure_functions_db as candidate\n"
            + "assert candidate.__file__ is not None and "
            + f"Path(candidate.__file__).resolve().is_relative_to(Path({source!r}))\n"
        )
        with (
            azurite(directory),
            configured("PATH", str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"]),
            configured("PYTHONPATH", source),
        ):
            settings = {
                "IsEncrypted": False,
                "Values": {
                    "AzureWebJobsStorage": os.environ["TEST_AZURITE_CONN_STR"],
                    "FUNCTIONS_WORKER_RUNTIME": "python",
                    "languageWorkers:python:defaultExecutablePath": sys.executable,
                },
            }
            (app / "local.settings.json").write_text(json.dumps(settings))
            log = directory / "logs/host.log"
            command = ["func", "start", "--verbose", "--port", str(port())]
            with child(command, log, app) as process:
                deadline = time.monotonic() + 120
                observed = False
                while time.monotonic() < deadline and process.poll() is None:
                    if (app / "host-smoke.db").exists():
                        with sqlite3.connect(app / "host-smoke.db") as connection:
                            try:
                                observed = (
                                    connection.execute(
                                        "SELECT COUNT(*) FROM trigger_state"
                                    ).fetchone()[0]
                                    > 0
                                )
                            except sqlite3.OperationalError:
                                observed = False
                    if observed:
                        break
                    time.sleep(0.5)
            text = log.read_text()
            succeeded = "Executed 'Functions.db_trigger_host_smoke' (Succeeded" in text
            failed = "(Failed" in text or "returned a non-None value" in text
            if not observed or not succeeded or failed:
                raise ServiceFailure(
                    "Functions host did not execute the decorated DB trigger successfully"
                )
    suite = ET.Element("testsuite", name="host-smoke")
    ET.SubElement(suite, "testcase", name="decorated_db_trigger_on_real_functions_host")
    ET.ElementTree(suite).write(directory / "junit.xml", encoding="unicode")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
