"""Run quality prerequisites inside the parent evidence runner."""

import subprocess


def main() -> int:
    return subprocess.run(["make", "check-steps"], check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
