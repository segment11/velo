# Bug 56 Bug 2 Same-Master CanRead Reset Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix Bug 2 from `doc/bug_reviews/bug_56_replication_module_review_round_1.md` so a same-master slave reset clears stale per-slot readability.

**Architecture:** Add a focused regression test in `LeaderSelectorTest`, then add one minimal state-reset call in `LeaderSelector.doResetAsSlave`. The same-master branch should preserve its no-teardown optimization while clearing `canRead` when it is stale.

**Tech Stack:** Java 21, Groovy/Spock tests, Gradle, JaCoCo.

---

### Task 1: Add failing regression test

**Files:**
- Modify: `src/test/groovy/io/velo/repl/LeaderSelectorTest.groovy`

**Step 1:** Add a Spock test that prepares one local slot, creates a mocked slave repl pair for `localhost:16379`, sets `oneSlot.canRead = true`, makes `checkMasterConfigMatch` succeed through a local Redis test server if available, calls `LeaderSelector.resetAsSlave('localhost', 16379)`, and asserts `oneSlot.canRead == false`.

**Step 2:** Run `./gradlew :test --tests "io.velo.repl.LeaderSelectorTest.test same master reset clears canRead"`.
Expected: FAIL because current code returns before clearing `canRead`.

### Task 2: Implement minimal production fix

**Files:**
- Modify: `src/main/java/io/velo/repl/LeaderSelector.java`

**Step 1:** In the same-master branch of `doResetAsSlave`, before `return`, add:

```java
if (oneSlot.isCanRead()) {
    oneSlot.setCanRead(false);
}
```

**Step 2:** Re-run the focused LeaderSelector test.
Expected: PASS.

### Task 3: Verify coverage and commit

**Step 1:** Run `./gradlew :test --tests "io.velo.repl.LeaderSelectorTest"`.

**Step 2:** Run `python3 scripts/jacoco_cover.py io.velo.repl.LeaderSelector 563 576 --src` and verify the same-master branch is covered.

**Step 3:** Update `doc/bug_reviews/bug_56_replication_module_review_round_1.md` with Bug 2 fix notes without overwriting existing uncommitted review feedback.

**Step 4:** Run `git diff --check -- src/main/java/io/velo/repl/LeaderSelector.java src/test/groovy/io/velo/repl/LeaderSelectorTest.groovy doc/bug_reviews/bug_56_replication_module_review_round_1.md docs/plans/2026-06-30-bug56-bug2-same-master-canread-reset.md`.

**Step 5:** Commit only the Bug 2 production/test/plan changes and the Bug 2 doc notes with message `fix: clear canread on same master reset`.
