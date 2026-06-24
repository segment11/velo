#!/usr/bin/env python3
"""
JavaDoc checker for Velo Java source files.

Usage:
    python scripts/javadoc_check.py <class-or-package>
    python scripts/javadoc_check.py io.velo.HostAndPort
    python scripts/javadoc_check.py io.velo.HostAndPort --src
    python scripts/javadoc_check.py io.velo.command
    python scripts/javadoc_check.py HostAndPort
    python scripts/javadoc_check.py io.velo.command --skip-override
    python scripts/javadoc_check.py io.velo.repl --recursive --skip-override --skip-getters-setters
    python scripts/javadoc_check.py io.velo.command --dir /path/to/project

Scans .java files under src/main/java, finds all public and protected members
(classes, interfaces, enums, records, methods, constructors, fields), and checks
whether each has a preceding /** ... */ JavaDoc comment.

Exit code: 0 if all checked members have JavaDoc, 1 if any are missing.
"""

import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

JAVA_KEYWORDS = frozenset({
    'if', 'while', 'for', 'switch', 'catch', 'return', 'new', 'super', 'this',
    'throw', 'throws', 'try', 'do', 'else', 'case', 'break', 'continue',
    'instanceof', 'synchronized', 'volatile', 'transient', 'native', 'default',
    'void', 'class', 'interface', 'enum', 'record', 'abstract', 'final',
    'static', 'strictfp', 'package', 'import', 'var', 'yield',
})


@dataclass
class Member:
    kind: str          # class, interface, enum, record, method, constructor, field
    name: str
    line: int          # 1-based
    modifier: str      # public or protected
    has_javadoc: bool = False
    source: str = ""


@dataclass
class FileResult:
    path: Path
    fqn: str
    members: list[Member] = field(default_factory=list)

    @property
    def missing(self) -> list[Member]:
        return [m for m in self.members if not m.has_javadoc]


# ---------------------------------------------------------------------------
# Phase 1: Preprocess — mask string/char/comment text, record JavaDoc blocks
# ---------------------------------------------------------------------------

def preprocess(path: Path) -> tuple[list[str], list[str], set[int]]:
    """Returns (raw_lines, cleaned_lines, javadoc_end_lines).

    cleaned_lines preserves all code characters but replaces string literals,
    char literals, and non-JavaDoc comment text with spaces, so that regex
    matching won't be confused by text that looks like code.

    javadoc_end_lines is a set of 0-based line indices where a ``*/`` that
    closes a ``/**`` block appears.
    """
    text = path.read_text(encoding="utf-8")
    raw_lines = text.split("\n")
    cleaned: list[str] = []
    javadoc_ends: set[int] = set()

    state = "code"  # code | block_comment | javadoc | string | char | text_block

    for lineno, raw in enumerate(raw_lines):
        chars = list(raw)
        result: list[str] = []
        j = 0
        n = len(chars)
        while j < n:
            c = chars[j]
            pair = "".join(chars[j : j + 2])
            triple = "".join(chars[j : j + 3])

            if state == "block_comment":
                if pair == "*/":
                    result.extend("  ")
                    j += 2
                    state = "code"
                else:
                    result.append(" ")
                    j += 1

            elif state == "javadoc":
                if pair == "*/":
                    result.extend("*/")
                    j += 2
                    state = "code"
                    javadoc_ends.add(lineno)
                else:
                    result.append(" ")
                    j += 1

            elif state == "string":
                if c == "\\":
                    result.extend("  " if j + 1 < n else " ")
                    j += 2
                elif c == '"':
                    result.append('"')
                    j += 1
                    state = "code"
                else:
                    result.append(" ")
                    j += 1

            elif state == "char":
                if c == "\\":
                    result.extend("  " if j + 1 < n else " ")
                    j += 2
                elif c == "'":
                    result.append("'")
                    j += 1
                    state = "code"
                else:
                    result.append(" ")
                    j += 1

            elif state == "text_block":
                if triple == '"""':
                    result.extend("   ")
                    j += 3
                    state = "code"
                else:
                    result.append(" ")
                    j += 1

            else:  # code
                if pair == "//":
                    break
                elif triple == '"""':
                    result.extend("   ")
                    j += 3
                    state = "text_block"
                elif triple == "/**":
                    result.extend("   ")
                    j += 3
                    state = "javadoc"
                elif pair == "/*":
                    result.extend("  ")
                    j += 2
                    state = "block_comment"
                elif c == '"':
                    result.append('"')
                    j += 1
                    state = "string"
                elif c == "'":
                    result.append("'")
                    j += 1
                    state = "char"
                else:
                    result.append(c)
                    j += 1

        cleaned.append("".join(result))

    return raw_lines, cleaned, javadoc_ends


# ---------------------------------------------------------------------------
# Phase 2: Detect public/protected members
# ---------------------------------------------------------------------------

TYPE_KW_RE = re.compile(r"\b(class|interface|enum|record|@interface)\s+(\w+)")
DECL_MOD_RE = re.compile(r"\b(public|protected)\b")
GETTER_SETTER_RE = re.compile(r"^(get|set|is)[A-Z]\w*$")


def detect_members(
    cleaned: list[str],
    javadoc_ends: set[int],
    raw_lines: list[str],
    skip_override: bool,
    skip_getters_setters: bool,
    is_groovy: bool = False,
) -> list[Member]:
    members: list[Member] = []
    depth = 0
    # Stack of (kind, name, depth_at_declaration) for enclosing types
    type_stack: list[tuple[str, str, int]] = []

    for i, cl in enumerate(cleaned):
        depth_before = depth

        # Pop types whose scope has ended (depth dropped to their declaration level)
        while type_stack and type_stack[-1][2] >= depth_before:
            type_stack.pop()

        in_interface = any(kind == "interface" for kind, _, _ in type_stack)
        class_names = {n for _, n, _ in type_stack}

        member = _try_member(cl, i, depth_before, class_names, in_interface, is_groovy)
        if member:
            if skip_override and _has_annotation(cleaned, i, "Override"):
                pass  # skip
            elif skip_getters_setters and member.kind == "method" and GETTER_SETTER_RE.match(member.name):
                pass  # skip
            else:
                member.has_javadoc = _check_javadoc(cleaned, i, javadoc_ends)
                member.source = raw_lines[i].rstrip()
                members.append(member)

        # Update brace depth
        depth += cl.count("{") - cl.count("}")
        if depth < 0:
            depth = 0

        # Push new type declarations seen on this line
        tm = TYPE_KW_RE.search(cl)
        if tm and "{" in cl:
            type_stack.append((tm.group(1), tm.group(2), depth_before))

    return members


def _try_member(
    cl: str,
    lineno: int,
    depth: int,
    class_names: set[str],
    in_interface: bool,
    is_groovy: bool = False,
) -> Member | None:
    """Try to detect a public/protected member on this cleaned line."""
    stripped = cl.strip()
    if not stripped or stripped.startswith("//"):
        return None

    # Only look at class/interface body level (depth 0 = top-level type, 1 = member)
    if depth > 1:
        return None

    # --- Groovy mode: classes and methods are public by default ---
    if is_groovy:
        return _try_groovy_member(cl, lineno, depth, class_names)

    # Must have public or protected keyword (or be an interface method at depth 1)
    mod_match = DECL_MOD_RE.search(cl)
    if not mod_match:
        if in_interface and depth == 1:
            return _try_interface_method(cl, lineno)
        return None

    modifier = mod_match.group(1)
    after_mod = cl[mod_match.end() :]

    # Skip if 'public'/'protected' is not at the start of a declaration
    # Heuristic: the text before the modifier should be only whitespace/annotations
    before_mod = cl[: mod_match.start()].strip()
    if before_mod and not before_mod.startswith("@"):
        return None

    # --- Type declaration (class, interface, enum, record) ---
    tm = TYPE_KW_RE.search(after_mod)
    if tm:
        return Member(tm.group(1), tm.group(2), lineno + 1, modifier)

    # --- Method or constructor: look for name( ---
    paren_pos = after_mod.find("(")
    if paren_pos != -1:
        before_paren = after_mod[:paren_pos]
        # If there's '=' before '(', it's a field init, not a method
        if "=" not in before_paren:
            name_m = re.search(r"(\w+)\s*$", before_paren.rstrip())
            if name_m:
                name = name_m.group(1)
                if name not in JAVA_KEYWORDS:
                    kind = "constructor" if name in class_names else "method"
                    return Member(kind, name, lineno + 1, modifier)

    # --- Field: name [= ...] or name; ---
    semi_pos = after_mod.find(";")
    eq_pos = after_mod.find("=")
    end_pos = min(
        p for p in (semi_pos, eq_pos) if p != -1
    ) if -1 not in (semi_pos, eq_pos) else max(semi_pos, eq_pos)
    if end_pos != -1:
        before_end = after_mod[:end_pos].rstrip()
        name_m = re.search(r"(\w+)\s*$", before_end)
        if name_m:
            name = name_m.group(1)
            if name not in JAVA_KEYWORDS:
                return Member("field", name, lineno + 1, modifier)

    return None


PRIVATE_RE = re.compile(r"\bprivate\b")
CONTROL_START_RE = re.compile(
    r"^(if|while|for|switch|catch|return|new|throw|throws|try|do|else|case|break|"
    r"continue|instanceof|synchronized|def\s|var\s|import\s|package\s)"
)


def _try_groovy_member(
    cl: str, lineno: int, depth: int, class_names: set[str]
) -> Member | None:
    """Detect implicit-public members in Groovy (classes and methods only)."""
    stripped = cl.strip()
    if not stripped or stripped.startswith("//") or stripped.startswith("@"):
        return None
    if stripped in ("{", "}", "};"):
        return None
    # Skip explicit private
    if PRIVATE_RE.search(cl):
        return None

    # Type declaration (class, interface, enum, record) — depth 0
    if depth == 0:
        tm = TYPE_KW_RE.search(cl)
        if tm and "{" in cl:
            return Member(tm.group(1), tm.group(2), lineno + 1, "public")
        return None

    # Class body (depth 1): methods and constructors only
    # Skip control-flow lines that happen to contain '('
    if CONTROL_START_RE.match(stripped):
        return None

    paren_pos = cl.find("(")
    if paren_pos == -1:
        return None
    # Skip if '=' before '(' (e.g., field init with closure call)
    if "=" in cl[:paren_pos]:
        return None

    before_paren = cl[:paren_pos].rstrip()
    name_m = re.search(r"(\w+)\s*$", before_paren)
    if not name_m:
        return None
    name = name_m.group(1)
    if name in JAVA_KEYWORDS:
        return None
    kind = "constructor" if name in class_names else "method"
    return Member(kind, name, lineno + 1, "public")


def _try_interface_method(cl: str, lineno: int) -> Member | None:
    """Detect implicit-public methods in interfaces (no explicit modifier)."""
    stripped = cl.strip()
    if not stripped or stripped.startswith("//") or stripped.startswith("@"):
        return None
    if stripped in ("{", "}", "};"):
        return None

    paren_pos = cl.find("(")
    if paren_pos == -1:
        return None
    if "=" in cl[:paren_pos]:
        return None

    before_paren = cl[:paren_pos].rstrip()
    name_m = re.search(r"(\w+)\s*$", before_paren)
    if not name_m:
        return None
    name = name_m.group(1)
    if name in JAVA_KEYWORDS:
        return None
    return Member("method", name, lineno + 1, "public")


# ---------------------------------------------------------------------------
# Phase 3: Check if a JavaDoc block precedes a member
# ---------------------------------------------------------------------------

def _check_javadoc(cleaned: list[str], member_line: int, javadoc_ends: set[int]) -> bool:
    """Scan backward from member_line (0-based), skipping annotations/blanks."""
    i = member_line - 1
    paren_depth = 0
    while i >= 0:
        cl = cleaned[i].strip()
        if paren_depth > 0:
            paren_depth += cl.count("(") - cl.count(")")
            i -= 1
            continue
        if cl == "":
            i -= 1
            continue
        if cl.startswith("@"):
            paren_depth += cl.count("(") - cl.count(")")
            i -= 1
            continue
        if i in javadoc_ends:
            return True
        return False
    return False


def _has_annotation(cleaned: list[str], member_line: int, name: str) -> bool:
    """Check if @name annotation appears on the lines before member_line."""
    i = member_line - 1
    paren_depth = 0
    while i >= 0:
        cl = cleaned[i].strip()
        if paren_depth > 0:
            paren_depth += cl.count("(") - cl.count(")")
            i -= 1
            continue
        if cl == "":
            i -= 1
            continue
        if cl.startswith("@"):
            if re.search(r"@" + name + r"\b", cl):
                return True
            paren_depth += cl.count("(") - cl.count(")")
            i -= 1
            continue
        return False
    return False


# ---------------------------------------------------------------------------
# File resolution
# ---------------------------------------------------------------------------

EXTENSIONS = (".java", ".groovy")

# Source directories from build.gradle main source set
MAIN_SOURCE_DIRS = [
    "src/main/java",
    "src/main/groovy",
    "dyn/src",
    "dyn/ctrl",
]


def resolve_target(
    target: str, src_dirs: list[Path], recursive: bool
) -> list[tuple[Path, str]]:
    """Resolve a class-or-package target across multiple source roots."""
    results: list[tuple[Path, str]] = []

    for src_dir in src_dirs:
        path_via_dots = src_dir / target.replace(".", "/")

        # Try class file: io.velo.HostAndPort -> src_dir/io/velo/HostAndPort.{java,groovy}
        for ext in EXTENSIONS:
            class_file = path_via_dots.with_suffix(ext)
            if class_file.is_file():
                results.append((class_file, target))

        # Try package directory: io.velo.command -> src_dir/io/velo/command/*.{java,groovy}
        if path_via_dots.is_dir():
            globber = path_via_dots.rglob if recursive else path_via_dots.glob
            for pattern in ("*.java", "*.groovy"):
                for f in sorted(globber(pattern)):
                    if f.is_file():
                        results.append((f, _fqn_from_path(f, src_dir)))

    # Deduplicate (same file could match from multiple patterns)
    seen = set()
    deduped = []
    for path, fqn in results:
        key = str(path)
        if key not in seen:
            seen.add(key)
            deduped.append((path, fqn))

    if deduped:
        return deduped

    # Try simple class name: HostAndPort -> recursive search
    if "." not in target:
        for src_dir in src_dirs:
            for ext in EXTENSIONS:
                for f in sorted(src_dir.rglob(f"{target}{ext}")):
                    if f.is_file():
                        deduped.append((f, _fqn_from_path(f, src_dir)))
        return deduped

    return []


def _fqn_from_path(path: Path, src_dir: Path) -> str:
    rel = path.relative_to(src_dir)
    return str(rel.with_suffix("")).replace("/", ".")


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

KIND_ORDER = {"class": 0, "interface": 1, "enum": 2, "record": 3,
              "constructor": 4, "field": 5, "method": 6}


def format_result(
    results: list[FileResult],
    show_source: bool,
) -> str:
    total = 0
    documented = 0
    missing = 0

    for r in results:
        total += len(r.members)
        documented += len(r.members) - len(r.missing)
        missing += len(r.missing)

    pct = f"{documented / total * 100:.0f}%" if total > 0 else "n/a"

    parts: list[str] = []
    parts.append(
        f"Files: {len(results)} | Members: {total} | "
        f"Documented: {documented}/{total} ({pct}) | Missing: {missing}"
    )
    parts.append("")

    for r in results:
        if not r.members:
            continue
        icon = "OK" if not r.missing else "!!"
        parts.append(f"[{icon}] {r.fqn}  ({len(r.members)} members, {len(r.missing)} missing)")

        if r.missing:
            sorted_missing = sorted(r.missing, key=lambda m: (KIND_ORDER.get(m.kind, 99), m.line))
            for m in sorted_missing:
                row = f"       L{m.line:<5} {m.modifier} {m.kind:<12} {m.name}"
                if show_source:
                    row += f"   {m.source.strip()[:100]}"
                parts.append(row)
            parts.append("")

    if missing == 0 and total > 0:
        parts.append("All public/protected members have JavaDoc.")
    elif total == 0:
        parts.append("No public/protected members found.")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Check JavaDoc coverage for Velo Java/Groovy classes"
    )
    parser.add_argument(
        "target",
        help="Fully qualified class name, simple class name, or package "
        "(e.g. io.velo.HostAndPort, HostAndPort, io.velo.command)",
    )
    parser.add_argument("--src", action="store_true", help="Show source text")
    parser.add_argument(
        "--skip-override", action="store_true", help="Skip @Override methods"
    )
    parser.add_argument(
        "--skip-getters-setters",
        action="store_true",
        help="Skip trivial getXxx()/setXxx()/isXxx() methods",
    )
    parser.add_argument(
        "--recursive", action="store_true", help="Include subpackages"
    )
    parser.add_argument(
        "--dir", type=str, default=".", help="Project root directory (default: current)"
    )
    parser.add_argument(
        "--java-only", action="store_true",
        help="Only scan .java files (default: scan all main source dirs)",
    )
    args = parser.parse_args()

    root = Path(args.dir).resolve()

    if args.java_only:
        src_dirs = [root / "src" / "main" / "java"]
    else:
        src_dirs = [root / d for d in MAIN_SOURCE_DIRS]
    src_dirs = [d for d in src_dirs if d.is_dir()]
    if not src_dirs:
        print("No source directories found under:", root, file=sys.stderr)
        sys.exit(1)

    files = resolve_target(args.target, src_dirs, args.recursive)
    if not files:
        print(f"No source files found for: {args.target}", file=sys.stderr)
        print("Searched under:", ", ".join(str(d) for d in src_dirs), file=sys.stderr)
        sys.exit(1)

    results: list[FileResult] = []
    for path, fqn in files:
        raw_lines, cleaned, javadoc_ends = preprocess(path)
        is_groovy = path.suffix == ".groovy"
        members = detect_members(
            cleaned, javadoc_ends, raw_lines,
            args.skip_override, args.skip_getters_setters, is_groovy,
        )
        results.append(FileResult(path=path, fqn=fqn, members=members))

    print(format_result(results, args.src))

    any_missing = any(r.missing for r in results)
    sys.exit(1 if any_missing else 0)


if __name__ == "__main__":
    main()
