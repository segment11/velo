# PGroup HLL Correctness Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix `PGroup` HyperLogLog command behavior so `PFCOUNT` uses union semantics, `PFMERGE` tolerates missing source keys, and HLL rewrites preserve TTL.

**Architecture:** Keep the fix local to `io.velo.command.PGroup`. Expand `PGroupTest` with regression cases that demonstrate Redis-compatible HLL behavior, then update the shared HLL load/save paths so both sync and async command branches use consistent semantics. Preserve existing command structure and async split-by-slot pattern.

**Tech Stack:** Java 21, Groovy/Spock, Gradle 8.14, HyperLogLog library

---

### Task 1: Add regression tests for HLL correctness

**Files:**
- Modify: `src/test/groovy/io/velo/command/PGroupTest.groovy`

**Step 1: Write the failing tests**

Add coverage for:
- `PFCOUNT a b` with overlapping members returning union cardinality rather than sum
- `PFMERGE dst a missing` succeeding when one source key is absent
- `PFADD` or `PFMERGE` on an expiring HLL preserving the key TTL

**Step 2: Run tests to verify they fail**

Run:

```bash
export JAVA_HOME=/home/kerry/.jdks/temurin-21.0.9
export PATH="$JAVA_HOME/bin:$PATH"
./gradlew :test --tests "io.velo.command.PGroupTest"
```

Expected: FAIL in the new HLL regression cases.

### Task 2: Implement minimal `PGroup` fixes

**Files:**
- Modify: `src/main/java/io/velo/command/PGroup.java`

**Step 1: Write minimal implementation**

Update `PGroup` so:
- `PFCOUNT` merges source HLLs into a temporary sketch and returns the merged count
- missing source keys are treated as empty sketches in both `PFCOUNT` and `PFMERGE`
- HLL save path preserves the existing `expireAt` value when rewriting an HLL key

**Step 2: Run `PGroup` tests to verify they pass**

Run:

```bash
export JAVA_HOME=/home/kerry/.jdks/temurin-21.0.9
export PATH="$JAVA_HOME/bin:$PATH"
./gradlew :test --tests "io.velo.command.PGroupTest"
```

Expected: PASS.

### Task 3: Verify adjacent command coverage

**Files:**
- Test: `src/test/groovy/io/velo/command/PGroupTest.groovy`

**Step 1: Run focused command tests**

Run:

```bash
export JAVA_HOME=/home/kerry/.jdks/temurin-21.0.9
export PATH="$JAVA_HOME/bin:$PATH"
./gradlew :test --tests "io.velo.command.PGroupTest"
```

Expected: PASS with the final code state.

### Task 4: Commit after review approval

**Files:**
- Create: `docs/plans/2026-03-11-pgroup-hll-correctness.md`
- Modify: `src/main/java/io/velo/command/PGroup.java`
- Modify: `src/test/groovy/io/velo/command/PGroupTest.groovy`

**Step 1: Commit**

After review approval:

```bash
git add docs/plans/2026-03-11-pgroup-hll-correctness.md src/main/java/io/velo/command/PGroup.java src/test/groovy/io/velo/command/PGroupTest.groovy
git commit -m "fix: correct pgroup hll behavior"
```
