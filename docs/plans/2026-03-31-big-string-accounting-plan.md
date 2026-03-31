# Big String Accounting Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `BigStringFiles` metrics and accounting reflect the actual bytes written and actual delete success.

**Architecture:** Keep file I/O behavior unchanged and tighten only the accounting invariants in `BigStringFiles`. Cover both write-slice accounting and failed-delete accounting with focused `BigStringFilesTest` regressions.

**Tech Stack:** Java 21, Groovy/Spock, Gradle, JaCoCo

---

### Task 1: Add failing sliced-write accounting regression

**Files:**
- Modify: `src/test/groovy/io/velo/persist/BigStringFilesTest.groovy`

**Step 1: Write the failing test**

Add a test that writes only a slice of a larger byte array through `writeBigStringBytes(uuid, bucketIndex, keyHash, bytes, offset, length)` and asserts:
- file content length equals `length`
- `diskUsage` increments by `length`
- `writeByteLengthTotal` increments by `length`

**Step 2: Run test to verify it fails**

Run:

```bash
./gradlew :test --tests "io.velo.persist.BigStringFilesTest.test sliced write accounting uses actual length"
```

Expected: fail because current accounting uses `bytes.length`.

### Task 2: Add failing delete-accounting regression

**Files:**
- Modify: `src/test/groovy/io/velo/persist/BigStringFilesTest.groovy`

**Step 1: Write the failing test**

Add a test that:
- creates one big-string file
- prevents deletion from succeeding
- calls `deleteBigStringFileIfExist(...)`
- asserts the method returns `false`
- asserts `bigStringFilesCount` and `diskUsage` are unchanged

**Step 2: Run test to verify it fails**

Run:

```bash
./gradlew :test --tests "io.velo.persist.BigStringFilesTest.test failed delete keeps accounting unchanged"
```

Expected: fail because current code decrements accounting before the delete result is known.

### Task 3: Fix write accounting

**Files:**
- Modify: `src/main/java/io/velo/persist/BigStringFiles.java`

**Step 1: Write minimal implementation**

- Replace `bytes.length` with `length` in the sliced write accounting path.

**Step 2: Run focused test**

Run:

```bash
./gradlew :test --tests "io.velo.persist.BigStringFilesTest.test sliced write accounting uses actual length"
```

Expected: pass.

### Task 4: Fix delete accounting

**Files:**
- Modify: `src/main/java/io/velo/persist/BigStringFiles.java`

**Step 1: Write minimal implementation**

- Move file-count, disk-usage, and delete-stat updates behind successful `file.delete()`.
- Keep return semantics unchanged.

**Step 2: Run focused test**

Run:

```bash
./gradlew :test --tests "io.velo.persist.BigStringFilesTest.test failed delete keeps accounting unchanged"
```

Expected: pass.

### Task 5: Run combined verification and inspect JaCoCo

**Files:**
- Inspect: `build/reports/jacocoHtml/io.velo.persist/BigStringFiles.java.html`

**Step 1: Run focused verification**

Run:

```bash
./gradlew :test --tests "io.velo.persist.BigStringFilesTest"
```

Expected: pass.

**Step 2: Inspect coverage**

Confirm the touched write and delete accounting lines in `BigStringFiles.java` are covered in JaCoCo.

**Step 3: Commit**

```bash
git add src/main/java/io/velo/persist/BigStringFiles.java src/test/groovy/io/velo/persist/BigStringFilesTest.groovy docs/plans/2026-03-31-big-string-accounting-design.md docs/plans/2026-03-31-big-string-accounting-plan.md
git commit -m "fix: correct big string accounting"
```
