#!/usr/bin/env python3
"""
JaCoCo line coverage checker for AI agents.

Usage:
    python scripts/jacoco_cover.py <fully.qualified.ClassName> <startLine> <endLine>
    python scripts/jacoco_cover.py io.velo.command.SGroup 1102 1230
    python scripts/jacoco_cover.py io.velo.command.SGroup 1102 1230 --src
    python scripts/jacoco_cover.py io.velo.command.SGroup 1102 1230 --dir /path/to/project

Finds the JaCoCo HTML report under build/reports/jacocoHtml/, parses the
source-level HTML, and prints a coverage summary for the given line range.
"""

import argparse
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from html.parser import HTMLParser


@dataclass
class LineInfo:
    line_number: int
    status: str  # covered, partial, not-covered, na
    branch_status: str | None  # e.g. "All 2 branches covered", "1 of 2 branches missed", None
    source_text: str


STATUS_MAP = {
    "fc": "covered",
    "nc": "not-covered",
    "pc": "partial",
}

BRANCH_CLASSES = {"bfc", "bpc", "bnc"}


class JacocoLineParser(HTMLParser):
    def __init__(self, start: int, end: int):
        super().__init__()
        self.start = start
        self.end = end
        self.lines: list[LineInfo] = []
        self._current_id = None
        self._current_classes: list[str] = []
        self._current_title = ""
        self._capture_text = False
        self._text_buf = ""

    def handle_starttag(self, tag, attrs):
        if tag != "span":
            return
        attr_dict = dict(attrs)
        classes = attr_dict.get("class", "").split()
        line_id = attr_dict.get("id", "")
        title = attr_dict.get("title", "")

        if line_id.startswith("L"):
            try:
                lineno = int(line_id[1:])
            except ValueError:
                return
            if self.start <= lineno <= self.end:
                self._current_id = lineno
                self._current_classes = classes
                self._current_title = title
                self._capture_text = True
                self._text_buf = ""

    def handle_data(self, data):
        if self._capture_text:
            self._text_buf += data

    def handle_endtag(self, tag):
        if tag != "span":
            return
        if self._current_id is not None and self._capture_text:
            line_status = "na"
            branch_status = None

            for cls in self._current_classes:
                base = cls.replace("b", "", 1) if cls in BRANCH_CLASSES else cls
                if base in STATUS_MAP:
                    line_status = STATUS_MAP[base]
                if cls in BRANCH_CLASSES and self._current_title:
                    branch_status = self._current_title

            self.lines.append(LineInfo(
                line_number=self._current_id,
                status=line_status,
                branch_status=branch_status,
                source_text=self._text_buf.strip(),
            ))
            self._current_id = None
            self._current_classes = []
            self._current_title = ""
            self._capture_text = False


def find_report_html(root: Path, fqn: str) -> Path | None:
    package_part = fqn.rsplit(".", 1)[0]
    simple = fqn.rsplit(".", 1)[1]
    candidates = [
        root / "build" / "reports" / "jacocoHtml" / package_part / f"{simple}.java.html",
        root / "build" / "reports" / "jacocoHtml" / package_part / f"{simple}.groovy.html",
        root / "build" / "reports" / "jacocoHtml" / package_part.replace(".", os.sep) / f"{simple}.java.html",
        root / "build" / "reports" / "jacocoHtml" / package_part.replace(".", os.sep) / f"{simple}.groovy.html",
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


def parse_lines(html: str, start: int, end: int) -> list[LineInfo]:
    parser = JacocoLineParser(start, end)
    parser.feed(html)
    return parser.lines


STATUS_MARK = {
    "covered": "✓",
    "partial": "~",
    "not-covered": "✗",
    "na": " ",
}


def format_output(lines: list[LineInfo], show_source: bool) -> str:
    if not lines:
        return "No lines found in the given range."

    covered = sum(1 for l in lines if l.status == "covered")
    partial = sum(1 for l in lines if l.status == "partial")
    not_covered = sum(1 for l in lines if l.status == "not-covered")
    total = len(lines)
    branch_lines = [l for l in lines if l.branch_status]

    parts: list[str] = []
    parts.append(f"Lines: {total} | Covered: {covered} | Partial: {partial} | Not-covered: {not_covered}")
    if branch_lines:
        bc = sum(1 for l in branch_lines if "covered" in (l.branch_status or "").lower() and "missed" not in (l.branch_status or "").lower())
        parts.append(f"Branches: {len(branch_lines)} lines with branches, {bc} fully covered")
    parts.append("")

    header = "Line  Status  Branch"
    if show_source:
        header += "  Source"
    parts.append(header)
    parts.append("-" * len(header))

    for l in lines:
        mark = STATUS_MARK.get(l.status, "?")
        branch = l.branch_status if l.branch_status else ""
        row = f"{l.line_number:>5}  {mark} {l.status:<13s} {branch}"
        if show_source:
            src = l.source_text[:120]
            row += f"  {src}"
        parts.append(row)

    parts.append("")
    if not_covered > 0:
        nc_lines = [str(l.line_number) for l in lines if l.status == "not-covered"]
        parts.append(f"Not-covered lines: {', '.join(nc_lines)}")
    if partial > 0:
        p_lines = [str(l.line_number) for l in lines if l.status == "partial"]
        parts.append(f"Partial lines: {', '.join(p_lines)}")

    return "\n".join(parts)


def main():
    parser = argparse.ArgumentParser(description="Check JaCoCo line coverage for a class")
    parser.add_argument("class_name", help="Fully qualified class name, e.g. io.velo.command.SGroup")
    parser.add_argument("start_line", type=int, help="Start line number (inclusive)")
    parser.add_argument("end_line", type=int, help="End line number (inclusive)")
    parser.add_argument("--src", action="store_true", help="Show source text for each line")
    parser.add_argument("--dir", type=str, default=".", help="Project root directory (default: current)")
    args = parser.parse_args()

    root = Path(args.dir).resolve()
    report = find_report_html(root, args.class_name)
    if report is None:
        print(f"JaCoCo report not found for {args.class_name}", file=sys.stderr)
        print(f"Searched under: {root / 'build' / 'reports' / 'jacocoHtml'}", file=sys.stderr)
        sys.exit(1)

    html = report.read_text(encoding="utf-8")
    lines = parse_lines(html, args.start_line, args.end_line)
    print(format_output(lines, args.src))


if __name__ == "__main__":
    main()
