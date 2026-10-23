from collections.abc import Sequence
from html import unescape
import re

from tools.verification_masks import masked

_ESCAPE = re.compile(
    r"%[0-9a-fA-F]{2}|\\(?:[\"'\\nrt]|u[0-9a-fA-F]{4})|&(?:#\d{1,8}|#x[0-9a-fA-F]{1,8}|[A-Za-z]{1,32});|\+"
)
_HINT = re.compile(
    r"(?i)password|passwd|secret|token|key|authorization|code|sig|sas|pwd|master|default|function|host|://"
)


def redact_encoded(text: str, patterns: Sequence[re.Pattern[str]], secrets: Sequence[str]) -> str:
    """Inspect decoded views while replacing only original secret value spans."""
    decoded = text
    offsets: list[tuple[int, int]] | None = None
    spans: list[tuple[int, int]] = []
    scanners = (*patterns, *(re.compile(re.escape(secret)) for secret in secrets))
    for level in range(4):
        if secrets or _HINT.search(decoded):
            for pattern in scanners:
                for match in pattern.finditer(decoded):
                    start, end = (
                        match.span("value") if "value" in pattern.groupindex else match.span()
                    )
                    if masked(decoded[start:end]):
                        continue
                    if (
                        decoded[start : start + 1] in {'"', "'"}
                        and decoded[end - 1 : end] == decoded[start]
                    ):
                        start, end = start + 1, end - 1
                    if start < end:
                        spans.append(
                            (offsets[start][0], offsets[end - 1][1]) if offsets else (start, end)
                        )
        escapes = list(_ESCAPE.finditer(decoded)) if level < 3 else []
        if not escapes:
            break
        if offsets is None:
            offsets = [(index, index + 1) for index in range(len(text))]
        characters: list[str] = []
        positions: list[tuple[int, int]] = []
        previous = 0
        for match in escapes:
            start, end = match.span()
            characters.append(decoded[previous:start])
            positions.extend(offsets[previous:start])
            value = match.group()
            if value.startswith("%"):
                character = chr(int(value[1:], 16))
            elif value.startswith(r"\u"):
                character = chr(int(value[2:], 16))
            elif value.startswith("\\"):
                character = {"n": "\n", "r": "\r", "t": "\t"}.get(value[1], value[1])
            else:
                character = " " if value == "+" else unescape(value)
            characters.append(character)
            positions.extend([(offsets[start][0], offsets[end - 1][1])] * len(character))
            previous = end
        characters.append(decoded[previous:])
        positions.extend(offsets[previous:])
        decoded, offsets = "".join(characters), positions
    merged: list[tuple[int, int]] = []
    for start, end in sorted(spans):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))
    parts: list[str] = []
    previous = 0
    for start, end in merged:
        parts.extend((text[previous:start], "[REDACTED]"))
        previous = end
    parts.append(text[previous:])
    return "".join(parts)
