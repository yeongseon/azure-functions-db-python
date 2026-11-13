"""Offline demo: %ENV_VAR% expansion next to percent-encoded URL credentials.

Run it with no database, Azure account, or credentials::

    python examples/percent_encoded_url.py

Every value below is synthetic. The script never connects to anything and
prints the URL with the password masked.
"""

from __future__ import annotations

from collections.abc import Mapping
import os
from unittest.mock import patch

from sqlalchemy.engine import make_url

from azure_functions_db.core import resolve_env_vars

# User ``app@example`` and secret ``s3/cr@t%`` are percent-encoded (@=%40, /=%2F,
# %=%25) so they survive inside the URL. ``%DB_HOST%`` is the only real token.
URL_TEMPLATE = "postgresql+psycopg://app%40example:s3%2Fcr%40t%25@%DB_HOST%:5432/orders"
SYNTHETIC_ENV: Mapping[str, str] = {"DB_HOST": "db.example.invalid"}
EXPECTED_EFFECTIVE_URL = (
    "postgresql+psycopg://app%40example:s3%2Fcr%40t%25@db.example.invalid:5432/orders"
)
EXPECTED_USERNAME = "app@example"
EXPECTED_SECRET = "s3/cr@t%"  # nosec B105  # synthetic demo value


def expand(template: str = URL_TEMPLATE, env: Mapping[str, str] = SYNTHETIC_ENV) -> str:
    """Expand ``%VAR%`` tokens once; percent-encoded bytes are left untouched."""
    with patch.dict(os.environ, dict(env)):
        return resolve_env_vars(template)


def masked(url: str) -> str:
    """Render a URL for logs: the password is replaced by ``***``."""
    return make_url(url).render_as_string(hide_password=True)


def main() -> None:
    effective = expand()
    parsed = make_url(effective)
    if effective != EXPECTED_EFFECTIVE_URL:
        raise SystemExit("percent-encoded URL was altered during expansion")
    if parsed.username != EXPECTED_USERNAME or parsed.password != EXPECTED_SECRET:
        raise SystemExit("driver would decode unexpected credentials")
    print("template :", masked(URL_TEMPLATE.replace("%DB_HOST%", "host")))
    print("effective:", masked(effective))
    print("user     :", parsed.username)
    print("password : <decoded once by SQLAlchemy, never printed>")


if __name__ == "__main__":
    main()
