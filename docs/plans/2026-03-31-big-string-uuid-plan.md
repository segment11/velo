# Big String UUID Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Stop big-string file identity from aliasing on `keyHash` collisions by assigning a real unique `uuid` to each big-string payload.

**Architecture:** Preserve the on-disk file naming shape `bucketIndex/<uuid>_<keyHash>`, but change the write paths so `uuid` is generated independently from `keyHash`. This keeps read, cleanup, and replication wiring stable while removing the collision bug.

**Tech Stack:** Java 21, Groovy/Spock, Gradle, JaCoCo

---

### Task 1: Add the failing regression for distinct UUIDs

**Files:**
- Modify: `src/test/groovy/io/velo/persist/OneSlotTest.groovy`

**Step 1: Write the failing test**

Add a test that:
- creates a `OneSlot`
- writes two big-string values through the write path with different keys but the same forced `keyHash`
- asserts the stored big-string metadata UUIDs are different
- asserts two different big-string files exist

**Step 2: Run test to verify it fails**

Run:

```bash
./gradlew :test --tests "io.velo.persist.OneSlotTest.test big string uuid is not key hash"
```

Expected: fail because the current code reuses `keyHash` as the UUID.

### Task 2: Fix `BaseCommand` big-string UUID generation

**Files:**
- Modify: `src/main/java/io/velo/BaseCommand.java`
- Test: `src/test/groovy/io/velo/BaseCommandTest.groovy` if needed

**Step 1: Write minimal implementation**

- Generate a fresh UUID from the existing sequence source before writing a big string.
- Pass that UUID into `writeBigStringBytes(...)`.
- Store that UUID into `setCompressedDataAsBigString(...)`.

**Step 2: Run focused tests**

Run:

```bash
./gradlew :test --tests "io.velo.BaseCommandTest"
```

Expected: pass.

### Task 3: Fix `OneSlot.put(...)` big-string UUID generation

**Files:**
- Modify: `src/main/java/io/velo/persist/OneSlot.java`
- Test: `src/test/groovy/io/velo/persist/OneSlotTest.groovy`

**Step 1: Write minimal implementation**

- Replace `keyHashAsUuid` with a real generated UUID.
- Keep the file name/path shape unchanged by continuing to pass both `uuid` and `keyHash`.
- Keep the binlog payload and encoded metadata using the generated UUID.

**Step 2: Run focused tests**

Run:

```bash
./gradlew :test --tests "io.velo.persist.OneSlotTest.test big string uuid is not key hash"
```

Expected: pass.

### Task 4: Verify cleanup and persisted scan behavior still work

**Files:**
- Existing tests only

**Step 1: Run regression tests**

Run:

```bash
./gradlew :test --tests "io.velo.persist.KeyLoaderTest.test interval delete big string files" --tests "io.velo.persist.OneSlotTest.test interval delete overwrite big string files"
```

Expected: pass.

### Task 5: Inspect coverage and commit

**Files:**
- Inspect: `build/reports/jacocoHtml/io.velo/BaseCommand.java.html`
- Inspect: `build/reports/jacocoHtml/io.velo.persist/OneSlot.java.html`

**Step 1: Confirm JaCoCo hit the changed lines**

Verify the new UUID-generation lines and the big-string write branches are covered.

**Step 2: Commit**

```bash
git add src/main/java/io/velo/BaseCommand.java src/main/java/io/velo/persist/OneSlot.java src/test/groovy/io/velo/persist/OneSlotTest.groovy docs/plans/2026-03-31-big-string-uuid-design.md docs/plans/2026-03-31-big-string-uuid-plan.md
git commit -m "fix: use unique big string uuid"
```
