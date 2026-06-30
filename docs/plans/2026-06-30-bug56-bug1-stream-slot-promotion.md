# Bug 56 Bug 1 Stream Slot Promotion Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix Bug 1 from `doc/bug_reviews/bug_56_replication_module_review_round_1.md` so a slot left in slave state without a slave repl pair is promoted during `resetAsMaster`.

**Architecture:** Add a focused regression test in `LeaderSelectorTest`, then add one fallback branch in `LeaderSelector.doResetAsMaster`. The branch treats `readonly=true` as residual slave state when `removeReplPairAsSlave()` returns false, and calls `OneSlot.resetAsMaster()` before the existing already-master path.

**Tech Stack:** Java 21, Groovy/Spock tests, Gradle, JaCoCo.

---

### Task 1: Add failing regression test

**Files:**
- Modify: `src/test/groovy/io/velo/repl/LeaderSelectorTest.groovy`

**Step 1:** Add a Spock test that prepares one local slot, marks it `readonly=true` and `canRead=false`, leaves it with no slave-side repl pair, calls `LeaderSelector.resetAsMaster(true)`, and asserts the slot becomes `readonly=false` and `canRead=true`.

**Step 2:** Run `./gradlew :test --tests "io.velo.repl.LeaderSelectorTest.test promote readonly stream slot without repl pair to master"`.
Expected: FAIL because current code logs already-master and leaves the slot readonly/not readable.

### Task 2: Implement minimal production fix

**Files:**
- Modify: `src/main/java/io/velo/repl/LeaderSelector.java`

**Step 1:** In `doResetAsMaster`, after the existing scale-up extra-slot branch and before the already-master branch, add `else if (oneSlot.isReadonly()) { oneSlot.resetAsMaster(); resetAsMasterCount = 0; }`.

**Step 2:** Re-run the focused LeaderSelector test.
Expected: PASS.

### Task 3: Verify coverage and commit

**Step 1:** Run `python3 scripts/jacoco_cover.py io.velo.repl.LeaderSelector 419 438 --src` after the test run and verify the new branch is covered.

**Step 2:** Run `git diff --check -- src/main/java/io/velo/repl/LeaderSelector.java src/test/groovy/io/velo/repl/LeaderSelectorTest.groovy doc/bug_reviews/bug_56_replication_module_review_round_1.md docs/plans/2026-06-30-bug56-bug1-stream-slot-promotion.md`.

**Step 3:** Commit only this bug fix and its review/plan docs with message `fix: promote slave slot without repl pair`.
