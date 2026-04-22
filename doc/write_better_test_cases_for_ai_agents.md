# Write Better Test Cases for AI Coding Agents

This document is for AI coding agents working in the Velo repository.

The goal is not to write more tests. The goal is to write tests that prove the changed behavior, exercise the changed branch, and keep the production design clean.

## Why This Doc Exists

Agents often make the same mistakes:
- writing a happy-path test for an error-path fix
- writing a test whose name says "overflow" while the test data never reaches the overflow branch
- treating "code looks reasonable" as enough without proving the changed lines executed
- adding production-only test hooks when a simpler test strategy would work

This repo expects a higher bar:
- TDD first
- relevant tests only
- JaCoCo confirmation for touched lines or branches

## Non-Negotiable Rules

1. Do not write production code before a failing test exists.
2. A passing test is not enough if it does not execute the changed branch.
3. Prefer the real entry path of the bug.
4. Do not add production API only to make testing easier.
5. Use the cheapest test setup that still proves the behavior.
6. Check JaCoCo after running relevant tests and confirm the touched branch was executed.

## TDD Workflow For Agents

For every feature, bug fix, refactor, or behavior change:

1. Write one failing test for the exact behavior you are changing.
2. Run that test and confirm it fails for the expected reason.
3. Make the minimal production change.
4. Re-run the relevant tests and confirm they pass.
5. Inspect JaCoCo and confirm the touched lines or branches were actually executed.
6. Only then consider the change complete.

If the test passes immediately, it did not prove the new behavior. Fix the test first.

## What Good Regression Tests Look Like

A good regression test:
- has a name that matches the exact branch or behavior
- sets up the smallest input that reaches the changed code path
- asserts the externally meaningful result
- fails before the fix and passes after it

A weak regression test:
- only exercises normal behavior
- uses unrelated large setup "just in case"
- checks broad success instead of the specific branch result
- never reaches the new guard, exception, or edge path

## Prefer Real Entry Paths

Prefer tests that enter through the same path that production uses.

Examples:
- For decode-path bugs, prefer crafted encoded bytes and call `decode()` or `iterate()`.
- For protocol bugs, prefer building the protocol input rather than mutating unrelated fields.
- For public API bugs, call the public method instead of reaching into internals first.

This gives stronger evidence and reduces tests that depend on implementation detail.

## How To Test Hard Branches Cheaply

Some branches are hard to reach through normal object growth. That does not mean skip TDD. It means choose a better trigger.

### Preferred strategies

1. Crafted input bytes

Use this when the branch depends on decoded or deserialized state.

Good for:
- malformed lengths
- invalid metadata
- decode edge cases

2. Small helper extraction

If a branch is real logic and extracting a small helper makes the production code cleaner, extract it and test it directly. Package-private or `@VisibleForTesting` access is acceptable when it improves design.

Good for:
- validation logic used in multiple places
- arithmetic or range checks
- small pure functions

3. Reflection

Use reflection only when the branch depends on internal state that is otherwise expensive or unnatural to build, and adding production-only mutators would make the design worse.

Good for:
- forcing impossible internal state
- driving one-off edge branches in tests

### Avoid

- building huge real datasets just to hit a simple guard branch
- adding `setForTestOnly(...)` style methods to production classes
- changing production visibility only to make tests easier when crafted input or reflection is enough

## Reflection vs Crafted Input vs `@VisibleForTesting`

Use this decision rule:

- If the bug comes from external bytes or serialized metadata: prefer crafted input.
- If the logic is a real reusable rule: prefer helper extraction or `@VisibleForTesting`.
- If the branch depends on rare private state and exposing it would harm design: use reflection.

### Rule of thumb

- Best: real entry path
- Good: small extracted helper
- Acceptable: reflection for rare branch forcing
- Worst: production API added only for tests

## JaCoCo Expectations

JaCoCo is evidence, not decoration.

After running relevant tests:
- open the JaCoCo report
- inspect the touched class
- confirm the new lines or branches were executed

Do not claim success from:
- a passing test with no branch evidence
- a test name that suggests coverage without actually reaching the branch
- indirect coverage assumptions

If you changed a guard like:

```java
if (size > Short.MAX_VALUE) {
    throw new IllegalStateException(...);
}
```

then the review bar is not "encode still works for 100 items". The review bar is "a test actually reached the `size > Short.MAX_VALUE` branch".

## Common Bad Patterns

### Bad: test name overclaims

```groovy
def 'test encode throws when size exceeds Short.MAX_VALUE'() {
    given:
    def rz = new RedisZSet()
    100.times { rz.add(it, 'member_' + it) }

    when:
    def encoded = rz.encode()

    then:
    noExceptionThrown()
}
```

This is not an overflow test. It is a normal encode test with a misleading name.

### Better: drive the real branch

- either build enough entries if that is cheap enough
- or use a cheaper branch-driving strategy that still proves the same guard

### Bad: huge setup for a simple arithmetic branch

Do not allocate massive state if the branch only depends on one internal number.

### Better: small targeted setup

Use reflection or crafted metadata to place the object near the boundary, then assert the exact exception or result.

### Bad: production-only test hooks

Avoid adding mutators that exist only for tests.

### Better: keep production design clean

Prefer:
- crafted bytes
- helper extraction
- reflection for rare cases

## Repo-Specific Test Guidance

### For bug fixes

- Start with the bug path, not the happy path.
- The test should fail before the fix for the exact reason the bug matters.
- The test should prove the specific guard, exception, or returned value added by the fix.

### For encode/decode classes

- Test both normal round-trip behavior and malformed/corrupt input behavior when relevant.
- If `decode()` validates a length, test invalid length explicitly.
- If `iterate()` mirrors `decode()` validation, test that mirror path too.

### For overflow guards

- Do not assume large constants mean huge tests are impossible.
- `Short.MAX_VALUE`-scale tests are often still practical.
- If `Integer.MAX_VALUE`-scale setup is unrealistic, use reflection or crafted metadata instead of skipping the branch test.

### For Redis protocol compatibility

- Prefer assertions on externally meaningful behavior:
  - reply type
  - reply content
  - encoded length
  - exception type and message when relevant

## Example Patterns

### Pattern 1: crafted corrupt input

```groovy
def 'iterate throws on invalid entry length'() {
    given:
    def rl = new RedisList()
    rl.addFirst('a'.bytes)
    def encoded = rl.encodeButDoNotCompress()
    def buffer = ByteBuffer.wrap(encoded)
    buffer.putShort(RedisList.HEADER_LENGTH, (short) -1)

    when:
    RedisList.iterate(encoded, false) { bytes, i ->
        false
    }

    then:
    def e = thrown(IllegalStateException)
    e.message.contains('Invalid list entry length')
}
```

### Pattern 2: reflection for rare internal state

```groovy
def 'put throws when capacity expansion overflows'() {
    given:
    def redisBF = RedisBF.decode(new RedisBF(true).encode())
    def listField = RedisBF.getDeclaredField('list')
    listField.setAccessible(true)
    def list = listField.get(redisBF)
    def one = list[0]
    def oneClass = Class.forName('io.velo.type.RedisBF$One')
    def capacityField = oneClass.getDeclaredField('capacity')
    capacityField.setAccessible(true)
    capacityField.set(one, (int) (Integer.MAX_VALUE / 2.0) + 1)

    when:
    redisBF.put('trigger')

    then:
    thrown(RuntimeException)
}
```

Use this pattern only when a real entry-path setup would be much more expensive or unnatural.

## Pre-Commit Checklist For Agents

Before claiming a test change is complete, verify:

- Did I write the failing test first?
- Did the test fail for the right reason?
- Does the test name match the branch it actually exercises?
- Does the setup reach the changed branch, not just nearby normal behavior?
- Did I choose the least-distorting test strategy?
- Did I run only the relevant tests?
- Did I inspect JaCoCo and confirm the touched lines or branches executed?
- If I used reflection, can I justify why it is better than adding production-only test hooks?

## Final Rule

Do not optimize for "a test exists".

Optimize for:
- proving the changed behavior
- keeping production code clean
- making regressions obvious
- leaving evidence in tests and JaCoCo that the bug path was really covered
