# HttpHeaderBody Content-Length Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `HttpHeaderBody` parse `Content-Length` case-insensitively so lowercase `content-length` requests keep reading the HTTP body correctly.

**Architecture:** Keep the fix narrow. Do not normalize all stored headers or change header retrieval semantics. Instead, preserve existing header storage and update `contentLength()` to resolve `Content-Length` with a case-insensitive lookup, covered by a focused regression test.

**Tech Stack:** Java 21, Groovy/Spock, Gradle 8.14

---

### Task 1: Add regression coverage for lowercase content-length

**Files:**
- Modify: `src/test/groovy/io/velo/decode/HttpHeaderBodyTest.groovy`

**Step 1: Write the failing test**

Add a test case that feeds:

```http
POST / HTTP/1.1
content-length: 4

1234
```

and asserts:
- `fullyRead` is `true`
- `contentLength() == 4`
- `body()` is `"1234"`

**Step 2: Run test to verify it fails**

Run:

```bash
export JAVA_HOME=/home/kerry/.jdks/temurin-21.0.9
export PATH="$JAVA_HOME/bin:$PATH"
./gradlew :test --tests "io.velo.decode.HttpHeaderBodyTest"
```

Expected: FAIL because `contentLength()` currently only reads exact-case `Content-Length`.

### Task 2: Implement minimal parser fix

**Files:**
- Modify: `src/main/java/io/velo/decode/HttpHeaderBody.java`

**Step 1: Write minimal implementation**

Update `contentLength()` so it:
- checks the existing exact-case lookup first
- falls back to scanning stored headers for a case-insensitive match on `Content-Length`
- preserves all existing return behavior for invalid, missing, or negative values

**Step 2: Run focused test to verify it passes**

Run:

```bash
export JAVA_HOME=/home/kerry/.jdks/temurin-21.0.9
export PATH="$JAVA_HOME/bin:$PATH"
./gradlew :test --tests "io.velo.decode.HttpHeaderBodyTest"
```

Expected: PASS.

### Task 3: Verify adjacent decode coverage

**Files:**
- Test: `src/test/groovy/io/velo/decode/RequestDecoderTest.groovy`

**Step 1: Run focused decode tests**

Run:

```bash
export JAVA_HOME=/home/kerry/.jdks/temurin-21.0.9
export PATH="$JAVA_HOME/bin:$PATH"
./gradlew :test --tests "io.velo.decode.HttpHeaderBodyTest" --tests "io.velo.decode.RequestDecoderTest"
```

Expected: PASS.

### Task 4: Commit after review approval

**Files:**
- Modify: `src/main/java/io/velo/decode/HttpHeaderBody.java`
- Modify: `src/test/groovy/io/velo/decode/HttpHeaderBodyTest.groovy`
- Create: `docs/plans/2026-03-11-http-header-body-content-length.md`

**Step 1: Commit**

After review approval:

```bash
git add docs/plans/2026-03-11-http-header-body-content-length.md src/main/java/io/velo/decode/HttpHeaderBody.java src/test/groovy/io/velo/decode/HttpHeaderBodyTest.groovy
git commit -m "fix: make HttpHeaderBody content-length lookup case-insensitive"
```
