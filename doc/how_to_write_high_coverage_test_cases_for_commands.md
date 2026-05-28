# How To Write High Coverage Test Cases For Commands

This document is for AI agents writing command tests in Velo.

The goal is to cover the command's real behavior and branches with focused Spock tests, not to create many small test
methods that make coverage hard to audit.

## Core Rule

For one Redis command, write one test method named:

```groovy
def 'test <command>'() {
```

Example:

```groovy
def 'test hgetdel'() {
```

Put all `when` / `then` cases for that command inside this one method.

Do not split one command into separate methods like:

- `test hgetdel error cases`
- `test hgetdel key too long`
- `test hgetdel hh mode`
- `test hgetdel missing key`

Those cases belong in `def 'test hgetdel'()`.

If the production code has internal helper paths such as `hgetex2`, still test them from the external command method
(`test hgetex`) unless there is no command-level entry path.

## What The Last Two HGroup Test Commits Showed

The last two commits added coverage for `hgetex`, `hgetex2`, and `hgetdel`. The useful lessons are:

- Cover both normal storage mode and `LocalPersist.instance.hashSaveMemberTogether = true`.
- Cover all parse and validation errors, not only happy paths.
- Cover missing-key branches such as `rhk == null` or `rhh == null`.
- Cover option variants such as `EX`, `PX`, `EXAT`, `PXAT`, and `PERSIST`.
- Verify persisted state after commands that mutate data or metadata.
- Use the command execution path (`hGroup.execute('...')`) so tests cover parsing, dispatch, validation, and behavior.
- Prefer `>key` and `>value` placeholders where existing tests use them to trigger key/value length guards.

The missing piece to keep consistent going forward: keep those cases grouped under the command's single test method.

## Command Test Checklist

For each command, read the production method first and make a branch checklist before writing assertions.

Cover these categories when the command has them:

1. Format errors:
   wrong argument count, missing required keyword, extra argument, field-count mismatch.

2. Parse errors:
   invalid integer, invalid long, invalid double, invalid cursor, unsupported option.

3. Length limits:
   key too long, field too long, value too long.

4. Missing data:
   key missing, metadata key missing, field missing, mixed existing and missing fields.

5. Wrong type:
   command runs against a key with another Redis type, if the command checks type.

6. Happy path:
   expected reply type, reply size, reply content, and ordering.

7. Mutation effects:
   deleted fields are gone, retained fields remain, expiry metadata changes, empty containers are removed when required.

8. Storage variants:
   normal split hash mode and HH mode (`hashSaveMemberTogether = true`) for hash commands; equivalent representation modes
   for other command groups.

9. Option matrix:
   every supported option gets at least one successful case and one invalid-value case when applicable.

10. Boundary behavior:
    zero, negative, max count, empty result, nil result, multi-bulk empty result, count larger than available data.

## Test Structure

Use one setup block, then a sequence of small `when` / `then` cases.

Pattern:

```groovy
def 'test hgetdel'() {
    given:
    def inMemoryGetSet = new InMemoryGetSet()
    def hGroup = new HGroup(null, null, null)
    hGroup.byPassGetSet = inMemoryGetSet
    hGroup.from(BaseCommand.mockAGroup())
    LocalPersist.instance.hashSaveMemberTogether = false

    when:
    def reply = hGroup.execute('hgetdel hashA')
    then:
    reply == ErrorReply.FORMAT

    when:
    reply = hGroup.execute('hgetdel hashA notfields 1 field1')
    then:
    reply == ErrorReply.SYNTAX

    when:
    reply = hGroup.execute('hgetdel hashA FIELDS abc field1')
    then:
    reply == ErrorReply.NOT_INTEGER

    when:
    reply = hGroup.execute('hgetdel hashA FIELDS 2 field1 field2')
    then:
    reply instanceof MultiBulkReply
    (reply as MultiBulkReply).replies.length == 2
    (reply as MultiBulkReply).replies[0] == NilReply.INSTANCE
    (reply as MultiBulkReply).replies[1] == NilReply.INSTANCE

    when:
    // prepare normal storage mode data, execute success case
    then:
    // assert reply and persisted state

    when:
    LocalPersist.instance.hashSaveMemberTogether = true
    // prepare HH mode data, execute success case
    then:
    // assert reply and persisted state

    cleanup:
    LocalPersist.instance.hashSaveMemberTogether = false
}
```

Keep each `when` focused. Reuse the same `reply` variable and rebuild test data before cases that mutate data.

## Ordering Inside The Method

Prefer this order:

1. Cheap validation and parse errors.
2. Missing-key and missing-metadata replies.
3. Normal storage happy path.
4. Normal storage partial-missing path.
5. Normal storage mutation-state verification.
6. Alternate storage mode happy path.
7. Alternate storage mode mutation-state verification.
8. Cleanup.

This order makes JaCoCo inspection easier because the test follows the production branches from guards to real work.

## Assertions Must Prove The Branch

Do not stop at `reply instanceof MultiBulkReply` when the branch has more observable behavior.

Assert:

- exact `ErrorReply`
- exact integer reply value
- multi-bulk reply length
- nil positions inside a multi-bulk reply
- returned bulk content
- persisted metadata after mutation
- removed fields no longer exist
- untouched fields still exist
- expiry timestamp changed or was removed

Example:

```groovy
def savedRHK = RedisHashKeys.decode(keysCv.cv().getCompressedData(), false)
!savedRHK.contains('field1')
!savedRHK.contains('field2')
savedRHK.contains('field3')
```

That is stronger than only checking the command returned two values.

## Cover Both Storage Modes When A Command Has Them

For hash commands, many code paths split between:

- `hashSaveMemberTogether = false`: `RedisHashKeys` plus per-field values
- `hashSaveMemberTogether = true`: `RedisHH`

If both paths exist in production, the command test should cover both in the same method.

Always reset global mode in `cleanup`:

```groovy
cleanup:
LocalPersist.instance.hashSaveMemberTogether = false
```

If a test changes other global state, restore that too.

## Use Real Command Entry

Prefer:

```groovy
reply = hGroup.execute('hgetdel hashA FIELDS 2 field1 field2')
```

Avoid directly calling private helpers or setting internal parser state unless the command has no practical public entry.

The command entry covers:

- command token parsing
- slot parsing
- dispatch
- argument validation
- data read/write behavior
- reply shape

## JaCoCo Workflow

After writing or changing command tests:

1. Run only the relevant focused test:

```bash
./gradlew :test --tests "io.velo.command.HGroupTest.test hgetdel"
```

2. Inspect the JaCoCo HTML report under `build/reports/jacocoHtml/`.

3. Open the touched command class and confirm the intended lines and branches are covered.

4. If a branch is still red or yellow, add a new `when` / `then` case inside the same command test method.

Do not create another method just because JaCoCo found another branch for the same command.

## Common Mistakes

- Splitting one command across many test methods.
- Testing happy path only, then claiming error branches are covered.
- Asserting reply type but not reply contents.
- Forgetting to verify storage after a mutating command.
- Covering normal hash mode but missing HH mode.
- Not rebuilding data before a mutating case.
- Leaving `LocalPersist.instance.hashSaveMemberTogether = true` after the test.
- Writing a test that passes before the production fix.
- Running the test but not checking JaCoCo.

## Final Rule

For command tests, coverage is not just line count. A good test proves the command's external Redis reply and the internal
state change for every meaningful branch, all organized in one `test <command>` method.

