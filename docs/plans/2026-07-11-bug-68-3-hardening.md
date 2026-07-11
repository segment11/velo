# Bug 68-3 Hardening Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the non-blocking correctness and coverage follow-ups from the review of commit `acc6127f`.

**Architecture:** The durable transition record is the serialization authority. A new role command may reuse a
compatible transition but must reject an unfinished opposite-role transition rather than overwrite it. Test-only
failure injection belongs in `RoleTransitionState.Store`, allowing coordinator persistence failures to be exercised
without changing production behavior.

**Tech Stack:** Java 21, Groovy/Spock, ActiveJ promises, Gradle, JaCoCo.

---

### Task 1: Guard overlapping opposite transitions

**Files:**
- Modify: `src/test/groovy/io/velo/repl/LeaderSelectorTest.groovy`
- Modify: `src/main/java/io/velo/repl/LeaderSelector.java`

1. Add promotion-over-demotion and demotion-over-promotion regression tests.
2. Run both tests and confirm they fail because the pending record is overwritten.
3. Add one helper that rejects an unfinished opposite target and use it in both entry paths.
4. Rerun the focused tests and confirm the original epoch/record remains unchanged and the node remains fenced.

### Task 2: Cover transition-store failures

**Files:**
- Modify: `src/test/groovy/io/velo/repl/LeaderSelectorTest.groovy`
- Modify: `src/main/java/io/velo/repl/RoleTransitionState.java`

1. Add deterministic tests for failure while saving `COMMITTING` and clearing converged state.
2. Run the tests and confirm they fail because store injection does not exist.
3. Add `@TestOnly` one-shot `save` and `clear` failure controls to `RoleTransitionState.Store`.
4. Rerun and confirm pre-publication save failure becomes `ABORTED`, while post-publication clear failure becomes
   `FAILED_FENCED`.

### Task 3: Documentation and verification

**Files:**
- Modify: `src/test/groovy/io/velo/repl/LeaderSelectorTest.groovy`
- Modify: `doc/bug_reviews/bug_68_replication_module_review_round_1.md`

1. Rename the stale rollback test to describe `FAILED_FENCED` publication failure.
2. Clarify that the implemented persisted path is `PREPARING -> COMMITTING`, with `PREPARED` reserved.
3. Run focused tests, inspect JaCoCo for the new branches, run `git diff --check`, and commit the follow-up.
