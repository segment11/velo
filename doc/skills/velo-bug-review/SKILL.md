---
name: velo-bug-review
description: Use when sweeping Velo modules or classes for bugs, fixing bugs recorded in doc/bug_reviews reports, verifying findings as the second AI agent, or starting a new review round after fixes. Triggers on "round N", "bug_N module review", "find bugs in io.velo.<package>", "fix bug N.M", "confirm bugs in this doc".
---

# Velo Bug Review/Fix Workflow

## Overview

Two adversarial AI agents sweep the codebase: **agent 1 = Author/Fixer**, **agent 2 = Reviewer**. One markdown report per round is the state machine — findings' Status fields are updated in place by whoever holds the pen. **Every stage has a MUST-READ list and a grep-able Compliance Gate. No evidence, no pass.**

Violating the letter of these rules is violating the spirit of these rules.

## State & Naming

- Allocate report ids from `doc/bug_reviews/STATE.md`. Never pick a number by memory — ids 59/60/32 were double-spent before STATE.md existed. Update STATE.md (next id + module coverage) when creating a report.
- File name: `doc/bug_reviews/bug_<N>_<module>_review_round_<M>.md`
- `round_M` = fresh sweep after the prior round's fixes. Verification / fix / re-review cycles **append sections to the same file**; they never consume a round number.
- Round M>1 must open with a delta declaration: link the prior round doc, state prior-round disposition counts ("X fixed / Y won't fix"), list items that must not be re-reported.

## Stage 1 — Discovery (agent 1)

MUST-READ before writing any finding:

1. `AGENTS.md` — build/test/JaCoCo commands
2. The target module's design doc (`doc/`, `docs/`)
3. Ground truth for semantics: Redis 7.2 official docs / source (web fetch when the module is a command group)
4. Prior rounds on the same module (per STATE.md) — never re-report REFUTED / NOT A BUG / won't-fix items

Compliance Gate — the report must contain:

- `## Scope` listing every file read **with method + line ranges**, every reference doc with URL, and a `Reviewed commit:` sha baseline
- Findings numbered `N.M`, each with `Severity` (enum) / `Class` / `Status` + subsections: `Cited files / lines` → `Root cause` → `Trigger / impact` → `Suggested fix direction`
- `## Notes / non-findings` (negative results are recorded)
- `## Reviewer checklist` — a reproduction recipe per finding

## Stage 2 — Verification (agent 2)

MUST-READ: the report under review, its cited lines, and prior-round refutations.

Evidence strength order: Spock temp spec output → live Redis 7.2 differential → JDK repro → static inspection. "Looks right" is banned.

Compliance Gate:

- Every verdict uses ONLY the Status Vocabulary below
- Every verdict carries pasted evidence (test output / transcript)
- Append `## Review Feedback` with per-finding verdicts + `### Recommended fix order`
- Update each finding's `Status` field in place — including REFUTED (kept, never deleted)

## Stage 3 — Response (agent 1)

Agree or disagree per finding. Disagreement requires new code citations, not opinion. Retract your own finding when the reviewer is right (`RETRACTED`, title struck through, evidence kept).

## Stage 4 — Fix (agent 1)

**REQUIRED SUB-SKILL:** `test-driven-development` (red-green-refactor). Read it before touching code.

MUST-READ:

1. `doc/write_better_test_cases_for_ai_agents.md` — TDD first, Non-Negotiable Rules, JaCoCo bar
2. `doc/how_to_write_high_coverage_test_cases_for_commands.md` — one `def 'test <command>'()` method per command (command bugs only)

Per bug, in order:

1. Rebuild the reviewer's temp spec as a permanent failing regression test — **first**
2. Watch it fail
3. Minimal fix (smallest diff that turns the test green)
4. `./gradlew :test --tests "<focused class>"` green
5. JaCoCo: confirm the changed lines/branches are covered (`scripts/jacoco_cover.py`)
6. One bug = one commit, message `fix: <lowercase one line>`

Compliance Gate — the Fix Log table must show, per bug: failing-test evidence → commit sha → focused test pass → JaCoCo confirmation. Missing any column = re-review rejects the fix.

## Stage 5 — Re-review (agent 2)

Verify all four fix evidences per commit. Append `Re-review` sections as many times as needed. Close with `## Residual Risk`.

## Status Vocabulary (ONLY these)

- Verdicts: `CONFIRMED (reproduced)` / `CONFIRMED (inspection)` / `PARTIALLY CONFIRMED` / `REFINED` / `REFUTED` / `NOT A BUG` / `RETRACTED`
- Dispositions: `fixed` / `won't fix` / `no change needed`
- Severity: `High` / `Medium` / `Low` (parenthetical qualifiers allowed)

## Red Flags — STOP and restart

- Writing a finding before finishing the MUST-READs
- Fix code written before the failing test exists
- A verdict without pasted evidence
- Reusing a bug id without checking STATE.md

| Excuse | Reality |
|---|---|
| "I already know TDD" | The repo bar (TDD first + JaCoCo on changed lines) is written in the docs; skipping makes the fix unreviewable |
| "Scope line numbers rot anyway" | They anchor verification; pair them with `Reviewed commit:` sha |
| "One combined commit is cleaner" | One bug one commit is what re-review parses |
| "The doc is long, I got the idea" | Non-Negotiable Rules live at the bottom of that doc |

## Template

Report skeleton: `templates/report.md` in this skill directory. Compliance fields are pre-seeded — an empty field is a visible violation.
