---
name: velo-design-plan
description: Use when planning or implementing a feature, refactor, or bug-N.M follow-up change in the Velo repo — writing a plan doc in docs/plans, reviewing/approving a plan as the second AI agent, executing an approved plan, or reviewing the resulting commits. Triggers on "write a plan", "review the plan", "implement <feature>", "refactor <module>", "plan doc", "round N review", "approve for execution".
---

# Velo Design/Plan Workflow

## Overview

Two adversarial AI agents deliver features/refactors: **agent 1 = Planner/Implementer**, **agent 2 = Reviewer**. The plan doc is the artifact under negotiation; **no code is written before `Approved for execution`**. The human arbitrates deadlocks and open design decisions.

Violating the letter of these rules is violating the spirit of these rules.

## Stage 0 — MUST-READ (BOTH agents, symmetric, before any writing)

1. Design doc for the target module: `doc/design/NN_<module>_design.md` (latest version — never draft a plan against a stale design)
2. **Codebase inventory sweep** — not casual reading: enumerate the affected paths with `file:line` anchors. Big features add a Command Path Audit (every entry point that can reach the change)
3. Prior plans for the same module (`docs/plans/`), and for bug-N.M type: the source report `doc/bug_reviews/bug_N_..._round_M.md`
4. Ground truth for command/protocol changes: Redis official docs / source

## Stage 1 — Write the plan (agent 1)

Use `templates/plan.md`. Size tiers: small (1–2 tasks, no extras) / large (adds `Current Behavior`, `Target Behavior`, `Command Path Audit`, `Open Review Question` lead sections + trailing `## Review Checklist` of invariants). A paired design doc (`templates/design.md`) is required only for architecture-level changes.

Compliance Gate — a plan without these is invalid:

- `## Current Code Findings` — every claim anchored `file:line` + code excerpt (absence = didn't read code = invalid)
- `Related:` lists the design docs actually read
- Header Goal/Architecture/Tech Stack; Architecture states the "do not change" boundary from code facts
- Every Task: Files (verb-prefixed) → TDD steps → exact runnable `./gradlew :test --tests "<full name>"` → `Expected: fail because <reason>` / `Expected: pass` (bare "fail" is banned)
- Final task commits code + tests + this plan (+ design doc, + bug report for bug-N.M type) with a conventional one-line message, then `git status --short` check
- Each task compiles and goes green independently — **no forward references** to symbols declared in later tasks

## Stage 2 — Consensus loop (max 5 rounds)

Per round, agent 2 writes a review (`templates/review-round.md`) after doing Stage 0 independently:

1. Resolved-verification table for the prior round's findings (against revised plan line numbers)
2. New findings numbered `R<k>-N1, R<k>-N2...` (separate from history). Verify every `Current Code Findings` citation against the real code — a stale/fabricated citation is Blocking. Hunt for blind spots the plan missed (own grep for call sites / derived paths)
3. Verdict: `APPROVED for execution` | `approvable after <findings>` | `not safe to execute`
4. Agent 1 responds per finding (disagreement needs fresh code evidence), revises by appending a Revision section (original text stays traceable)

**Every revision requires re-review** — past rounds each introduced ~1 new defect in revision. Loop until APPROVED, **max 5 rounds**; then STOP — write irreconcilable disagreements into `Decision points for the reviewer` (both sides' arguments + code evidence) and escalate to the human. Human rulings count as round-0 verdicts; one more agent-2 round after incorporating them releases the plan. On APPROVED, set plan header `Status: Approved for execution`.

## Stage 3 — Execute (agent 1)

**REQUIRED SUB-SKILL:** `executing-plans`. Task by task: failing test → watch fail → minimal implementation → green → JaCoCo on changed lines (`scripts/jacoco_cover.py`) → one concern one commit. Never batch fixes. Deviations from the plan get written back into the plan before committing.

## Stage 4 — Commit review (agent 2)

Reuse the evidence rules and verdict vocabulary of skill `velo-bug-review` (Stage 2/5). Additionally verify: commits match what the plan promised (task boundaries, no scope creep), no forward-reference violations, plan `Status:` updated with validation (commit hashes + suite results appended as `## Validation status (<date>)`). For bug-N.M type: fix status written back to the source bug_reviews report.

## Red Flags — STOP

- Writing or reviewing a plan before finishing Stage 0
- Coding with plan Status ≠ `Approved for execution`
- A verdict or finding without `file:line` evidence from the reviewer's own reads
- "We converged, skip the re-review" after any revision
- Reviewing a plan by reading only the plan

| Excuse | Reality |
|---|---|
| "The design doc is stale anyway" | Then updating it is task zero of the plan, not a reason to skip it |
| "I know this code, no need to re-read" | R5 of oneslot found a whole audit blind area the planner 'knew' |
| "Small change, one round is enough" | Rounds are cheap pre-code; rework post-commit is not |
| "Citations are approximately right" | Stale citations are Blocking findings — they mislead the reviewer |
