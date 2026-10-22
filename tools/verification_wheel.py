from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import sysconfig
import tempfile

from tools.verification_process import run


def main() -> int:
    directory = Path("test-results/wheel-smoke/logs")
    with tempfile.TemporaryDirectory(prefix="db-wheel-") as temporary:
        root = Path(temporary)
        code = run(
            [sys.executable, "-m", "build", "--no-isolation", "--outdir", str(root / "dist")],
            directory / "build.log",
        )
        if code:
            return code
        wheels = list((root / "dist").glob("*.whl"))
        if len(wheels) != 1:
            return 1
        subprocess.run(
            [sys.executable, "-m", "venv", "--system-site-packages", str(root / "venv")], check=True
        )
        python = str(root / "venv/bin/python")
        packages = (
            root
            / "venv/lib"
            / f"python{sys.version_info.major}.{sys.version_info.minor}"
            / "site-packages"
        )
        (packages / "verification-dependencies.pth").write_text(
            sysconfig.get_path("purelib") + "\n"
        )
        code = run(
            [python, "-m", "pip", "install", "--no-deps", "--no-index", str(wheels[0])],
            directory / "install.log",
        )
        if code:
            return code
        script = (
            "from pathlib import Path; import azure_functions_db as a; "
            "from sqlalchemy import create_engine,text; "
            f"assert Path(a.__file__).resolve().is_relative_to(Path({str(root)!r})); "
            f"url='sqlite:///{root}/smoke.db'; e=create_engine(url); "
            "c=e.connect(); c.execute(text('CREATE TABLE items (id INTEGER PRIMARY KEY)')); "
            "c.commit(); c.close(); "
            "w=a.DbWriter(url=url,table='items'); w.insert(data={'id':1}); w.close(); "
            "r=a.DbReader(url=url,table='items'); "
            "assert r.get(pk={'id':1}) == {'id':1}; r.close(); "
            "print('Installed wheel SQLite read/write smoke passed outside checkout')"
        )
        return run([python, "-I", "-c", script], directory / "smoke.log")


if __name__ == "__main__":
    raise SystemExit(main())
