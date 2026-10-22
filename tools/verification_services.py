from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import os
from pathlib import Path
import shutil
import socket
import sys
import tempfile
import time
import uuid

from tools.verification_environment import container_runtime
from tools.verification_process import child, run

BACKENDS = ("sqlite", "postgres", "mysql", "mssql", "azurite")
STORAGE_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)


class MissingPrerequisite(RuntimeError):
    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(detail)


class ServiceFailure(RuntimeError):
    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(detail)


def port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def storage_connection(blob: int, queue: int, table: int) -> str:
    return (
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        f"AccountKey={STORAGE_KEY};"
        f"BlobEndpoint=http://127.0.0.1:{blob}/devstoreaccount1;"
        f"QueueEndpoint=http://127.0.0.1:{queue}/devstoreaccount1;"
        f"TableEndpoint=http://127.0.0.1:{table}/devstoreaccount1;"
    )


@contextmanager
def configured(variable: str, value: str) -> Iterator[None]:
    previous = os.environ.get(variable)
    os.environ[variable] = value
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(variable, None)
        else:
            os.environ[variable] = previous


def preflight(backend: str, directory: Path, wait: bool = False) -> None:
    variable = "TEST_AZURITE_CONN_STR" if backend == "azurite" else f"TEST_{backend.upper()}_URL"
    if not os.environ.get(variable, "").strip():
        raise MissingPrerequisite(f"Missing required {variable}")
    deadline = time.monotonic() + (120 if wait else 0)
    while True:
        code = run(
            [
                sys.executable,
                "-m",
                "tools.verification_probe",
                backend,
                str(directory / "probe.json"),
            ],
            directory / "logs/readiness.log",
            10,
            quiet=True,
        )
        if code == 0:
            return
        if time.monotonic() >= deadline:
            raise ServiceFailure(f"{backend} connection readiness failed (exit {code})")
        time.sleep(1)


@contextmanager
def azurite(directory: Path) -> Iterator[None]:
    if shutil.which("azurite") is None:
        raise MissingPrerequisite("azurite is required for integration")
    with tempfile.TemporaryDirectory(prefix="db-azurite-") as temporary:
        blob, queue, table = port(), port(), port()
        command = [
            "azurite",
            "--silent",
            "--skipApiVersionCheck",
            "--location",
            temporary,
            "--blobHost",
            "127.0.0.1",
            "--queueHost",
            "127.0.0.1",
            "--tableHost",
            "127.0.0.1",
            "--blobPort",
            str(blob),
            "--queuePort",
            str(queue),
            "--tablePort",
            str(table),
        ]
        with configured("TEST_AZURITE_CONN_STR", storage_connection(blob, queue, table)):
            with child(command, directory / "logs/azurite.log"):
                preflight("azurite", directory, wait=True)
                run(["azurite", "--version"], directory / "logs/version.log", 10, quiet=True)
                yield


@contextmanager
def database(backend: str, directory: Path) -> Iterator[None]:
    if backend == "sqlite":
        with tempfile.TemporaryDirectory(prefix="db-sqlite-") as temporary:
            with configured("TEST_SQLITE_URL", f"sqlite:///{temporary}/integration.db"):
                yield
        return
    runtime = container_runtime()
    if runtime is None:
        raise MissingPrerequisite("docker/podman with reachable daemon required for " + backend)
    name = "db-verification-" + uuid.uuid4().hex
    password = "Verify1!" + uuid.uuid4().hex
    endpoint = port()
    configurations = {
        "postgres": (
            "postgres:16",
            5432,
            ["POSTGRES_USER=testuser", f"POSTGRES_PASSWORD={password}", "POSTGRES_DB=testdb"],
            f"postgresql+psycopg://testuser:{password}@127.0.0.1:{endpoint}/testdb",
        ),
        "mysql": (
            "mysql:8",
            3306,
            [
                f"MYSQL_ROOT_PASSWORD={password}",
                "MYSQL_DATABASE=testdb",
                "MYSQL_USER=testuser",
                f"MYSQL_PASSWORD={password}",
            ],
            f"mysql+pymysql://testuser:{password}@127.0.0.1:{endpoint}/testdb",
        ),
        "mssql": (
            "mcr.microsoft.com/mssql/server:2022-latest",
            1433,
            ["ACCEPT_EULA=Y", f"MSSQL_SA_PASSWORD={password}"],
            f"mssql+pyodbc://sa:{password}@127.0.0.1:{endpoint}/master?"
            "driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes",
        ),
    }
    image, container_port, variables, url = configurations[backend]
    command = [
        runtime,
        "run",
        "--detach",
        "--name",
        name,
        "--label",
        "db.verification=owned",
        "-p",
        f"127.0.0.1:{endpoint}:{container_port}",
    ]
    for variable in variables:
        command += ["-e", variable]
    try:
        with configured(f"TEST_{backend.upper()}_URL", url):
            if run([*command, image], directory / "logs/startup.log", 240):
                raise ServiceFailure(backend + " container startup failed")
            yield
    finally:
        run([runtime, "logs", name], directory / "logs/service.log", 10, quiet=True)
        if run([runtime, "rm", "-f", name], directory / "logs/cleanup.log", 30):
            raise ServiceFailure(backend + " owned container cleanup failed")
