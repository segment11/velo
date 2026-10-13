# Velo Opencode Skills

This directory documents the two project-specific [opencode](https://opencode.ai) skills used when working on the Velo codebase. They encode the repo's mandatory two-agent (author/reviewer) workflows.

The live skills are installed under `.opencode/skills/` at the repo root. The copies here are reference documentation — always keep them in sync with the installed ones.

## Skills Overview

| Skill | Directory | Purpose |
|---|---|---|
| `velo-bug-review` | `.opencode/skills/velo-bug-review/` | Sweep modules for bugs, verify/fix findings recorded in `doc/bug_reviews/`, run new review rounds |
| `velo-design-plan` | `.opencode/skills/velo-design-plan/` | Plan, review, approve, and execute features/refactors via plan docs in `docs/plans/` |

## How Opencode Loads Skills

- Skills are discovered from the project's `.opencode/skills/<skill-name>/SKILL.md` (each directory contains a `SKILL.md` with YAML frontmatter: `name` + `description`).
- When your request matches a skill's `description` triggers, opencode loads that skill's instructions into the session automatically. You can also load one explicitly by asking, e.g. "use the velo-bug-review skill".
- Skill files can reference extra resources in their own directory (both skills here ship `templates/` used as report/plan skeletons).

## velo-bug-review

**Use for:** finding bugs in a module/class, verifying a bug report as the second AI agent, fixing confirmed bugs, or starting a new review round after fixes.

**Typical prompts:**

- "Do a round 2 persist module review"
- "Find bugs in io.velo.repl"
- "Confirm the bugs in doc/bug_reviews/bug_61_replication_review_round_1.md"
- "Fix bug 61.3"

**Workflow summary (see `velo-bug-review/SKILL.md` for details):**

1. **Discovery (agent 1)** — read the MUST-READ list (`AGENTS.md`, module design doc, Redis 7.2 ground truth, prior rounds), then write `doc/bug_reviews/bug_<N>_<module>_review_round_<M>.md` using `templates/report.md`. Allocate ids from `doc/bug_reviews/STATE.md`.
2. **Verification (agent 2)** — reproduce or refute each finding with pasted evidence, update `Status` fields in place, append `## Review Feedback`.
3. **Response (agent 1)** — agree/disagree per finding with fresh code citations.
4. **Fix (agent 1)** — per confirmed bug, TDD: permanent failing regression test first → minimal fix → focused `./gradlew :test --tests "..."` green → JaCoCo confirmation via `scripts/jacoco_cover.py` → one bug one commit (`fix: <lowercase one line>`).
5. **Re-review (agent 2)** — verify fix evidence, append `Re-review` sections and `## Residual Risk`.

Key artifacts: reports live in `doc/bug_reviews/`, id allocation in `doc/bug_reviews/STATE.md`.

## velo-design-plan

**Use for:** planning or implementing a feature/refactor, writing/reviewing/approving a plan doc, executing an approved plan, or reviewing the resulting commits. Also covers bug-N.M follow-up changes that need more than a direct fix.

**Typical prompts:**

- "Write a plan for <feature>"
- "Review the plan in docs/plans/xxx.md"
- "Implement <feature> per the approved plan"
- "Approve for execution"

**Workflow summary (see `velo-design-plan/SKILL.md` for details):**

1. **Stage 0 MUST-READ (both agents)** — module design doc in `doc/design/`, codebase inventory with `file:line` anchors, prior plans in `docs/plans/`, Redis ground truth.
2. **Write the plan (agent 1)** — use `templates/plan.md` (+ `templates/design.md` for architecture-level changes). Every plan needs `## Current Code Findings` with `file:line` anchors and TDD task steps with exact test commands.
3. **Consensus loop (max 5 rounds)** — agent 2 reviews per `templates/review-round.md` (findings `R<k>-N`, verdict `APPROVED for execution` / `approvable after <findings>` / `not safe to execute`); agent 1 revises; every revision is re-reviewed. Deadlocks escalate to the human.
4. **Execute (agent 1)** — only when plan `Status: Approved for execution`. Task by task: failing test → minimal implementation → green → JaCoCo → one concern one commit.
5. **Commit review (agent 2)** — verify commits match the plan, update plan `Status` and, for bug-N.M type, write fix status back to the source bug report.

Key artifacts: plans live in `docs/plans/`, design docs in `doc/design/`.

## Working With Two Agents

Both skills assume two adversarial sessions (fresh context each):

- **Agent 1 (author/planner/fixer)** — starts from your request, holds the pen on the report/plan/code.
- **Agent 2 (reviewer)** — started with a verify/review request against the doc agent 1 produced, e.g. "review the plan in docs/plans/xxx.md".

Run them as separate opencode sessions so the reviewer's context is not contaminated by the author's reasoning.

## Maintenance

If you update a skill in `.opencode/skills/`, copy the changed files here so this documentation stays accurate:

```bash
cp -r .opencode/skills/velo-bug-review/. doc/skills/velo-bug-review/
cp -r .opencode/skills/velo-design-plan/. doc/skills/velo-design-plan/
```
