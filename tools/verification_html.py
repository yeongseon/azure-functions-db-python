from collections.abc import Callable
from html import escape, unescape
from html.parser import HTMLParser


class ReportText(HTMLParser):
    """Extract report content, including decoded pytest-html data attributes."""

    def __init__(self, sanitize: Callable[[str], str]) -> None:
        super().__init__(convert_charrefs=True)
        self.sanitize = sanitize
        self.parts: list[str] = []
        self.hidden = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.hidden = tag in {"script", "style"}
        for key, value in attrs:
            if value:
                self.parts.append(
                    self.sanitize(value)
                    if key == "data-jsonblob"
                    else self.sanitize(f"{key}={value}")
                )

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style"}:
            self.hidden = False

    def handle_data(self, data: str) -> None:
        if not self.hidden:
            self.parts.append(self.sanitize(unescape(data)))


def static_report(text: str, sanitize: Callable[[str], str]) -> str:
    prefix = (
        '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Redacted report</title>'
        "</head><body><pre>"
    )
    suffix = "</pre></body></html>"
    if text.startswith(prefix) and text.endswith(suffix):
        return prefix + escape(sanitize(unescape(text[len(prefix) : -len(suffix)]))) + suffix
    parser = ReportText(sanitize)
    parser.feed(text)
    parser.close()
    content = "\n".join(parser.parts)
    for _ in range(4):
        sanitized = sanitize(content)
        if sanitized == content:
            break
        content = sanitized
    return prefix + escape(content) + suffix
