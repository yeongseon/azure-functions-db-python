import re


def masked(value: str) -> bool:
    value = value.strip("\"'{}")
    password = value.rsplit(":", 1)[-1]
    return bool(
        re.fullmatch(r"\*{3,}|x{5,}|\[redacted\]|\[masked\]|<redacted>", password, re.IGNORECASE)
    )
