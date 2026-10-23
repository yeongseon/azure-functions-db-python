from collections.abc import Sequence
from html import unescape
import re


def redact_encoded(text: str, patterns: Sequence[re.Pattern[str]], secrets: Sequence[str]) -> str:
    """Decode only inspection views; replace value spans in the original representation."""
    decoded = text
    offsets = [(index, index + 1) for index in range(len(text))]
    spans: list[tuple[int, int]] = []
    for level in range(4):
        for pattern in (*patterns, *(re.compile(re.escape(secret)) for secret in secrets)):
            for match in pattern.finditer(decoded):
                start, end = match.span("value") if "value" in pattern.groupindex else match.span()
                if (
                    decoded[start : start + 1] in {'"', "'"}
                    and decoded[end - 1 : end] == decoded[start]
                ):
                    start, end = start + 1, end - 1
                if start < end:
                    spans.append((offsets[start][0], offsets[end - 1][1]))
        if level == 3:
            break
        characters: list[str] = []
        positions: list[tuple[int, int]] = []
        index = 0
        while index < len(decoded):
            width, character = 1, decoded[index]
            if re.fullmatch(r"%[0-9a-fA-F]{2}", decoded[index : index + 3]):
                width, character = 3, chr(int(decoded[index + 1 : index + 3], 16))
            elif decoded[index : index + 2] in {r"\"", r"\'", r"\\", r"\n", r"\r", r"\t"}:
                width = 2
                character = {"n": "\n", "r": "\r", "t": "\t"}.get(
                    decoded[index + 1], decoded[index + 1]
                )
            elif re.fullmatch(r"\\u[0-9a-fA-F]{4}", decoded[index : index + 6]):
                width, character = 6, chr(int(decoded[index + 2 : index + 6], 16))
            elif entity := re.match(r"&(?:#\d+|#x[0-9a-fA-F]+|[A-Za-z]+);", decoded[index:]):
                width, character = len(entity[0]), unescape(entity[0])
            elif character == "+":
                character = " "
            characters.extend(character)
            positions.extend([(offsets[index][0], offsets[index + width - 1][1])] * len(character))
            index += width
        decoded, offsets = "".join(characters), positions
    merged: list[tuple[int, int]] = []
    for start, end in sorted(spans):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))
    for start, end in reversed(merged):
        text = text[:start] + "[REDACTED]" + text[end:]
    return text
