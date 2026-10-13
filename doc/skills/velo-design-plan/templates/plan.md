# <Title> Implementation Plan
Date: YYYY-MM-DD
Status: Draft for review
Source: doc/bug_reviews/bug_N_<module>_review_round_M.md (Bug N.M)   <!-- bug-N.M type only -->
Related: <design docs / prior plans / reports actually read — repo-relative paths>

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** <one sentence; bug-N.M type cites the source report finding>
**Architecture:** <implementation strategy + explicit "do not change" boundary from code facts>
**Tech Stack:** Java 21, Groovy/Spock, Gradle, JaCoCo

<!-- Large plans insert here: ## Current Behavior / ## Target Behavior / ## Command Path Audit / ## Open Review Question -->

## Current Code Findings

<!-- every claim anchored; absence of this section invalidates the plan -->
- `src/main/java/io/velo/<File>.java:<lines>` — <what it does today>
```java
// excerpt
```

---

### Task 1: <imperative title>

**Files:**
- Modify: `src/main/java/io/velo/...`
- Test: `src/test/.../<Xxx>Test.groovy`
- Reference: `doc/design/NN_<module>_design.md`

**Step 1: Write the failing test**
<test intent / sketch>

**Step 2: Run test to verify it fails**
```bash
./gradlew :test --tests "io.velo.<Xxx>Test.test <exact name>"
```
Expected: fail because <specific reason — bare "fail" is banned>

**Step 3: Write minimal implementation**
<smallest change that turns the test green>

**Step 4: Run test to verify it passes**
```bash
./gradlew :test --tests "io.velo.<Xxx>Test.test <exact name>"
```
Expected: pass

**Step 5: Check JaCoCo coverage**
```bash
python3 scripts/jacoco_cover.py <class> <start> <end> --src
```
Expected: changed lines/branches covered

### Task N (final): Commit

**Step 1:** `git add <explicit file list — MUST include this plan (+ design doc if paired, + bug report if bug-N.M type)>`
**Step 2:** `git commit -m "fix:|feat:|refactor: <lowercase imperative one line>"`
**Step 3:** `git status --short` → Expected: only intended files

---

## Review Checklist   <!-- large plans required, small plans optional -->

- <invariant, one line each; reviewer checks implementation wording against these>

## Review Feedback (appended per consensus round)

<!-- use templates/review-round.md; rounds append here; revisions append "## Revision <k> — Response to Round <k>" sections -->

## Validation status (<date>)   <!-- appended post-execution -->

- <phase>: commit `<sha>` — <suite result, e.g. 25/25 green in ~8 min; skipped cases justified>
