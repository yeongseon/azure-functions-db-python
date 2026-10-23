from collections.abc import Sequence
import re


def redact_encoded(text: str, patterns: Sequence[re.Pattern[str]], secrets: Sequence[str]) -> str:
    """Map credential spans from up to three decoded views onto original characters."""
    decoded = text
    offsets = [(index, index + 1) for index in range(len(text))]
    spans: list[tuple[int, int]] = []
    for _ in range(3):
        characters: list[str] = []
        positions: list[tuple[int, int]] = []
        index = 0
        while index < len(decoded):
            width = 3 if re.fullmatch(r"%[0-9a-fA-F]{2}", decoded[index : index + 3]) else 1
            character = (
                chr(int(decoded[index + 1 : index + 3], 16)) if width == 3 else decoded[index]
            )
            characters.append(" " if character == "+" and width == 1 else character)
            positions.append((offsets[index][0], offsets[index + width - 1][1]))
            index += width
        decoded, offsets = "".join(characters), positions
        for pattern in (*patterns, *(re.compile(re.escape(secret)) for secret in secrets)):
            for match in pattern.finditer(decoded):
                start, end = offsets[match.start()][0], offsets[match.end() - 1][1]
                if "%" in text[start:end] or "+" in text[start:end]:
                    spans.append((start, end))
    merged: list[tuple[int, int]] = []
    for start, end in sorted(spans):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))
    for start, end in reversed(merged):
        text = text[:start] + "[REDACTED]" + text[end:]
    return text
