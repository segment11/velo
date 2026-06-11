# Bug 44: GEO Commands Review Round 1

Author: AI agent 1
Date: 2026-06-01

## Scope

Redis docs fetched from `https://redis.io/docs/latest/commands/?group=geo` and related command pages list the GEO
category commands as:

- `GEOADD`
- `GEODIST`
- `GEOHASH`
- `GEOPOS`
- `GEORADIUS`
- `GEORADIUS_RO`
- `GEORADIUSBYMEMBER`
- `GEORADIUSBYMEMBER_RO`
- `GEOSEARCH`
- `GEOSEARCHSTORE`

Velo's support table marks the deprecated `GEORADIUS*` commands unsupported, so this review focused on the commands
currently marked supported in `doc/redis_command_support.md`: `GEOADD`, `GEODIST`, `GEOHASH`, `GEOPOS`,
`GEOSEARCH`, and `GEOSEARCHSTORE`.

Reference docs:

- `https://redis.io/docs/latest/develop/data-types/geospatial/`
- `https://redis.io/docs/latest/commands/geosearch/`
- `https://redis.io/docs/latest/commands/geosearchstore/`
- `https://redis.io/docs/latest/commands/geoadd/`

## Bug 1: `GEOSEARCHSTORE STOREDIST` is rejected and store mode accepts read-only output flags

Severity: Medium

Files:

- `src/main/java/io/velo/command/GGroup.java:545-671`
- `src/main/java/io/velo/command/GGroup.java:742-747`

Code excerpt:

```java
private Reply geosearch(byte[][] dd, byte[] dstKeyBytes, SlotWithKeyHash dstS) {
    ...
    boolean isWithDist = false;
    boolean isWithHash = false;
    boolean isWithCoord = false;
    ...
    } else if ("withdist".equalsIgnoreCase(arg)) {
        isWithDist = true;
    } else if ("withhash".equalsIgnoreCase(arg)) {
        isWithHash = true;
    } else if ("withcoord".equalsIgnoreCase(arg)) {
        isWithCoord = true;
    } else {
        return ErrorReply.SYNTAX;
    }
    ...
    if (isNeedStoreToDstKey) {
        var dstRg = new RedisGeo();
        for (var result : resultList) {
            // not sorted, todo
            dstRg.add(result.member(), result.p.lon(), result.p.lat());
        }
```

Root cause:

`GEOSEARCH` and `GEOSEARCHSTORE` share one parser, but their option sets are not the same. Redis
`GEOSEARCHSTORE` supports `[STOREDIST]` and does not support the read reply modifiers `WITHCOORD`, `WITHDIST`, or
`WITHHASH`. Velo has the inverse behavior in store mode: `STOREDIST` falls into the unknown-option branch and returns
syntax error, while `WITHDIST`, `WITHHASH`, and `WITHCOORD` are accepted and ignored because store mode always writes
coordinates.

Impact:

- Valid Redis command `GEOSEARCHSTORE dst src ... STOREDIST` fails in Velo.
- Invalid Redis commands such as `GEOSEARCHSTORE dst src ... WITHDIST` can mutate `dst` and return an integer instead
  of failing.
- Clients using `STOREDIST` to store result distances cannot use Velo's advertised `GEOSEARCHSTORE` support.

Suggested fix:

Track whether `dstKeyBytes != null`. In store mode, accept `STOREDIST`, reject `WITH*` flags, and store distances when
`STOREDIST` is present. In read mode, reject `STOREDIST`.

## Bug 2: `GEOSEARCH` and `GEOSEARCHSTORE` allow missing units for `BYRADIUS` and `BYBOX`

Severity: Medium

Files:

- `src/main/java/io/velo/command/GGroup.java:614-663`

Code excerpt:

```java
} else if ("byradius".equalsIgnoreCase(arg)) {
    ...
    byRadius = Double.parseDouble(new String(dd[i + 1]));
    i++;
    ...
    // maybe has unit
    if (dd.length > i + 1) {
        var unitString = new String(dd[i + 1]);
        byRadiusUnit = RedisGeo.Unit.fromString(unitString);
        if (byRadiusUnit != RedisGeo.Unit.UNKNOWN) {
            byRadius = byRadiusUnit.toMeters(byRadius);
            i++;
        }
    }
    hasByRadius = true;
} else if ("bybox".equalsIgnoreCase(arg)) {
    ...
    byBoxWidth = Double.parseDouble(new String(dd[i + 1]));
    byBoxHeight = Double.parseDouble(new String(dd[i + 2]));
    i += 2;
    ...
    // maybe has unit
    if (dd.length > i + 1) {
        var unitString = new String(dd[i + 1]);
        byBoxUnit = RedisGeo.Unit.fromString(unitString);
        if (byBoxUnit != RedisGeo.Unit.UNKNOWN) {
            byBoxWidth = byBoxUnit.toMeters(byBoxWidth);
            byBoxHeight = byBoxUnit.toMeters(byBoxHeight);
            i++;
        }
    }
    hasByBox = true;
}
```

Root cause:

Redis syntax requires a unit for both search shapes: `BYRADIUS radius <M | KM | FT | MI>` or
`BYBOX width height <M | KM | FT | MI>`. Velo treats the unit as optional and defaults to meters if there is no
recognized next token.

Impact:

- Invalid requests such as `GEOSEARCH key FROMLONLAT 0 0 BYRADIUS 100` can execute as a 100 meter search instead of
  returning syntax error.
- Invalid requests such as `GEOSEARCH key FROMLONLAT 0 0 BYBOX 10 10` can execute as a 10 meter by 10 meter box.
- `GEOSEARCHSTORE` has the same issue and can write a destination key from malformed input.

Suggested fix:

After parsing the numeric shape arguments, require one additional argument and require it to parse as a known
`RedisGeo.Unit`. Return syntax error when the unit is missing or unknown.

## Bug 3: `GEOSEARCH` and `GEOSEARCHSTORE` do not support `COUNT count ANY`

Severity: Low

Files:

- `src/main/java/io/velo/command/GGroup.java:575-587`

Code excerpt:

```java
} else if ("count".equalsIgnoreCase(arg)) {
    if (i + 1 >= dd.length) {
        return ErrorReply.SYNTAX;
    }
    try {
        count = Integer.parseInt(new String(dd[i + 1]));
        if (count <= 0) {
            return ErrorReply.VALUE_NOT_POSITIVE;
        }
        i++;
    } catch (NumberFormatException e) {
        return ErrorReply.NOT_INTEGER;
    }
}
```

Root cause:

Redis supports `COUNT count [ANY]` for both `GEOSEARCH` and `GEOSEARCHSTORE`. Velo parses the integer count and then
leaves a following `ANY` token to be processed by the main option loop, where it is rejected as syntax error.

Impact:

- Valid Redis commands using `COUNT n ANY` fail in Velo.
- This affects both read queries and store queries because they share the parser.

Suggested fix:

After parsing `count`, optionally consume a following `ANY` token. Velo can initially treat `ANY` as an accepted
compatibility flag even if the current full-scan implementation does not use it to stop early.

## Bug 4: Duplicate sort direction options are accepted instead of rejected

Severity: Low

Files:

- `src/main/java/io/velo/command/GGroup.java:559-574`

Code excerpt:

```java
boolean isDesc = false;
...
if ("asc".equalsIgnoreCase(arg)) {
    isDesc = false;
} else if ("desc".equalsIgnoreCase(arg)) {
    isDesc = true;
}
```

Root cause:

Redis syntax defines sort direction as an optional one-of: `[ASC | DESC]`. Velo only stores the latest direction in a
single boolean, so inputs such as `ASC DESC`, `DESC ASC`, or repeated `ASC ASC` are accepted and the last value wins.

Impact:

- Malformed client requests can return valid-looking data instead of a syntax error.
- `GEOSEARCHSTORE` can mutate the destination key from a request Redis would reject.

Suggested fix:

Track a `hasOrder` boolean. If either `ASC` or `DESC` appears after a direction was already seen, return syntax error.

## Bug 5: `FROMLONLAT`, radius, width, and height are not range-validated

Severity: Medium

Files:

- `src/main/java/io/velo/command/GGroup.java:588-663`
- `src/main/java/io/velo/type/RedisGeo.java:201-204`

Code excerpt:

```java
} else if ("fromlonlat".equalsIgnoreCase(arg)) {
    ...
    fromLon = Double.parseDouble(new String(dd[i + 1]));
    fromLat = Double.parseDouble(new String(dd[i + 2]));
    i += 2;
    ...
} else if ("byradius".equalsIgnoreCase(arg)) {
    ...
    byRadius = Double.parseDouble(new String(dd[i + 1]));
    i++;
    ...
} else if ("bybox".equalsIgnoreCase(arg)) {
    ...
    byBoxWidth = Double.parseDouble(new String(dd[i + 1]));
    byBoxHeight = Double.parseDouble(new String(dd[i + 2]));
    i += 2;
```

Root cause:

`GEOADD` now validates longitude and latitude against `RedisGeo.GEO_LONG_*` and `RedisGeo.GEO_LAT_*`, but
`GEOSEARCH FROMLONLAT` does not. The parser also accepts zero or negative radius, width, and height values. Those
values flow into distance and bounding-box calculations.

Impact:

- Queries from impossible center coordinates can return results based on non-Redis-compatible math.
- Negative radius always filters out results; negative box dimensions invert the computed bounds and return empty
  results. Redis-compatible behavior should reject invalid shape sizes rather than silently returning an empty result.
- Store mode can remove or overwrite the destination key through `saveRedisGeo(empty, dstS)` after malformed input.

Suggested fix:

Validate `FROMLONLAT` with the same bounds as `GEOADD`. Validate `BYRADIUS`, `BYBOX width`, and `BYBOX height` as
strictly positive finite numbers before converting units.

---

## Reviewer Notes (AI agent 2)

Reviewer: AI agent 2
Date: 2026-06-01

### Bug 1: CONFIRMED

Status: **Confirmed**

Verified against code at `GGroup.java:545-672`. The `geosearch()` method is shared between `GEOSEARCH` and
`GEOSEARCHSTORE` (called at line 358 for store mode with `dstKeyBytes != null`). The parser at lines 664-669 accepts
`WITHDIST`, `WITHHASH`, `WITHCOORD` unconditionally — there is no check for store mode. There is no `STOREDIST`
handling anywhere in the codebase (grep confirmed zero hits for `storedist`/`STOREDIST`).

Confirmed findings:
1. `STOREDIST` is not recognized — falls through to `else` branch at line 670 returning `ErrorReply.SYNTAX`.
2. `WITHDIST`/`WITHHASH`/`WITHCOORD` are accepted in store mode (lines 664-669) but have no effect since store mode
   (lines 742-747) always writes coordinates via `dstRg.add(result.member(), result.p.lon(), result.p.lat())`.
3. The suggested fix direction is correct — branch on `dstKeyBytes != null` to differentiate store vs read mode
   option sets.

Severity: Medium is appropriate. Data corruption risk from accepting invalid `WITH*` flags in store mode is real but
limited to the destination key.

### Bug 2: CONFIRMED

Status: **Confirmed**

Verified against `GGroup.java:614-663`. The unit parsing at lines 628-636 (BYRADIUS) and 653-662 (BYBOX) uses a
`if (dd.length > i + 1)` guard followed by `Unit.fromString()` — if the next token is absent or not a valid unit,
the code silently keeps the default `RedisGeo.Unit.M` (meters) and continues. No error is returned.

Confirmed findings:
1. Unit is treated as optional — missing unit defaults to meters with no error.
2. Redis protocol requires a unit; clients omitting it get silently accepted with wrong semantics if they intended
   a different unit (though defaulting to meters happens to be the same as Redis for the `m` unit).
3. The real risk is when a user writes `BYRADIUS 100 km` but misspells `km` as `kmm` — the `kmm` falls through the
   option loop to the `else` at line 670 and returns syntax error, but `BYRADIUS 100` (no unit at all) silently
   passes. This asymmetry confirms the bug.

Severity: Medium is appropriate.

### Bug 3: CONFIRMED

Status: **Confirmed**

Verified against `GGroup.java:575-587`. After parsing `count` and incrementing `i`, the loop advances. If the next
token is `ANY`, it reaches line 570, `arg` becomes `"any"`, and none of the `if/else if` branches match it (there is
no `"any"` case). It falls through to the `else` at line 670 and returns `ErrorReply.SYNTAX`.

Confirmed findings:
1. `COUNT n ANY` fails with syntax error — the `ANY` token is not consumed.
2. The suggested fix is correct — after incrementing `i` for count, peek at the next token and consume it if it
   equals `"any"` (case-insensitive). Since Velo's implementation is a full scan, `ANY` can be accepted as a no-op.

Severity: Low is appropriate — this is a compatibility gap but not a data integrity issue.

### Bug 4: CONFIRMED

Status: **Confirmed**

Verified against `GGroup.java:559-574`. The parser stores only the latest direction via `isDesc = false` (ASC) or
`isDesc = true` (DESC). There is no guard against setting direction twice. Inputs like `ASC DESC` are accepted with
`isDesc = true` (last wins).

Confirmed findings:
1. Duplicate/repeated direction tokens are silently accepted.
2. Redis 7.x rejects `GEOSEARCH key FROMMEMBER m BYRADIUS 1 m ASC DESC` with syntax error.
3. Suggested fix is correct — add a `hasOrder` boolean and reject if set again.

Severity: Low is appropriate. Cosmetic input validation gap with no data corruption risk.

### Bug 5: CONFIRMED

Status: **Confirmed**

Verified against `GGroup.java:588-663` and `RedisGeo.java:201-204`.

- `FROMLONLAT` at lines 588-603: `fromLon` and `fromLat` are parsed via `Double.parseDouble` with no range check.
  Compare to `geoadd()` at lines 413-416 which validates against `GEO_LONG_MAX/MIN` and `GEO_LAT_MAX/MIN`.
- `BYRADIUS` at line 622: `byRadius` accepts any double including 0 and negatives.
- `BYBOX` at lines 646-647: `byBoxWidth` and `byBoxHeight` accept any double including 0 and negatives.

Confirmed findings:
1. `FROMLONLAT` missing lon/lat bounds check — confirmed by comparing with `geoadd()` validation at lines 413-416.
2. `BYRADIUS` accepts 0 or negative — a zero radius matches nothing (no harm but wrong error message), a negative
   radius in meters after unit conversion always filters out results (empty result silently returned).
3. `BYBOX` with negative dimensions flows into `RedisGeo.isWithinBox()` which could produce incorrect results.
4. Store mode with negative/zero shapes writes an empty result to the destination key via `saveRedisGeo` (line 750),
   which calls `removeDelay` when empty (line 378-380), potentially deleting an existing key.

Severity: Medium is appropriate — the store-mode key deletion from invalid input is the most concerning aspect.

---

## Summary

| Bug | Status    | Severity | Reviewer Notes |
|-----|-----------|----------|----------------|
| 1   | Confirmed | Medium   | STOREDIST missing, WITH* incorrectly accepted in store mode |
| 2   | Confirmed | Medium   | Unit treated as optional, silently defaults to meters |
| 3   | Confirmed | Low      | COUNT n ANY not parsed, ANY token causes syntax error |
| 4   | Confirmed | Low      | Duplicate ASC/DESC accepted, last value wins |
| 5   | Confirmed | Medium   | No range validation on FROMLONLAT, BYRADIUS, BYBOX dimensions |

All 5 bugs are confirmed. No bugs were refuted. The root cause analysis and suggested fixes in the original report are
accurate and actionable.

---

## Review Feedback: Bug 1 Fixed Commit

Reviewer: AI agent 1
Date: 2026-06-01
Commit: `2b474042` (`fix: GEOSEARCHSTORE accept STOREDIST, reject WITH* flags in store mode`)

### Summary

The commit addresses Bug 1 correctly. `GGroup.geosearch()` now distinguishes read mode from store mode with
`dstKeyBytes != null`, accepts `STOREDIST` only for `GEOSEARCHSTORE`, rejects `WITHDIST`/`WITHHASH`/`WITHCOORD` in
store mode, and rejects `STOREDIST` in `GEOSEARCH` read mode.

For `STOREDIST`, the destination is now written with `RedisZSet` through `ZGroup.saveRedisZSet(...)`. This matches
Redis behavior: `GEOSEARCHSTORE ... STOREDIST` stores members in a sorted set with scores equal to their distance from
the center in the same unit as the query shape.

### Strengths

- The parser now separates the two command option sets instead of silently accepting read-only output flags in store
  mode.
- `STOREDIST` stores a zset instead of a geo container, which is the correct data type for distance scores.
- The fix preserves the existing cross-slot/cross-worker storage pattern by adding both direct and async save paths.
- The focused `GGroupTest` covers the key syntax behavior: `STOREDIST` accepted in store mode, `STOREDIST` rejected
  in read mode, and `WITH*` flags rejected in store mode.

### Concerns

No blocking production-code issues found in the Bug 1 fix.

The remaining concern is test strength. The new positive `STOREDIST` assertion only checks that the reply is an
`IntegerReply`; it does not verify the count, the stored destination type, or the stored zset scores. A regression that
returned an integer but stored wrong scores could pass this test.

### Verification

- Ran `./gradlew :test --tests "io.velo.command.GGroupTest"`: passed.
- Checked JaCoCo with `python3 scripts/jacoco_cover.py io.velo.command.GGroup 560 788 --src`: the new parser branches
  for `STOREDIST` read/store mode and store-mode `WITH*` rejection are covered.
- Checked JaCoCo with `python3 scripts/jacoco_cover.py io.velo.command.ZGroup 429 440 --src`: the zset save path used
  by `STOREDIST` is covered.

### Follow-ups

- Pre-commit follow-up: add one stronger assertion for `GEOSEARCHSTORE ... STOREDIST`, preferably `ZSCORE zzz m0` or
  `ZRANGE zzz 0 -1 WITHSCORES`, to confirm that the destination is a zset and the score is in the requested unit.
- Post-commit follow-up: add async/cross-worker coverage for `STOREDIST` if cross-slot store behavior is a compatibility
  target for this command.

---

## Review Feedback: Bug 2 Fixed Commit

Reviewer: AI agent 1
Date: 2026-06-01
Commit: `405af68e` (`fix: require unit for GEOSEARCH/GEOSEARCHSTORE BYRADIUS and BYBOX`)

### Summary

The commit fixes Bug 2 correctly. `GGroup.geosearch()` now requires a unit token after `BYRADIUS radius` and after
`BYBOX width height`, rejects missing units, rejects unknown units, and only converts dimensions after a known
`RedisGeo.Unit` is parsed.

This matches the Redis command syntax, where both shapes require one of `M`, `KM`, `FT`, or `MI`.

### Strengths

- The parser now fails early when the radius or box shape is missing its required unit.
- Unknown units now return `ErrorReply.SYNTAX` instead of leaving the shape in default meters.
- Existing valid calls were updated to include explicit units, so the tests still cover the successful `BYBOX` path.
- The tests cover missing and unknown units for both `BYRADIUS` and `BYBOX`.

### Concerns

No blocking production-code issues found in the Bug 2 fix.

The only residual concern is test scope. Bug 2 affected both `GEOSEARCH` and `GEOSEARCHSTORE`, and the new negative
tests exercise the shared parser through `GEOSEARCH`. Because `GEOSEARCHSTORE` rewrites the argument array before
calling the same parser, this is probably sufficient for parser correctness, but it does not directly prove that a
malformed store request avoids mutating the destination key.

### Verification

- Ran `./gradlew :test --tests "io.velo.command.GGroupTest"`: passed.
- Checked JaCoCo with `python3 scripts/jacoco_cover.py io.velo.command.GGroup 638 689 --src`: the new missing-unit
  and unknown-unit branches for `BYRADIUS` and `BYBOX` are covered.

### Follow-ups

- Pre-commit follow-up: consider adding one `GEOSEARCHSTORE` negative test for a missing unit and assert the existing
  destination key remains unchanged. This would cover the mutation risk described in the original Bug 2 impact.

---

## Review Feedback: Bug 3 Fixed Commit

Reviewer: AI agent 1
Date: 2026-06-01
Commit: `ac127590` (`fix: support COUNT n ANY in GEOSEARCH and GEOSEARCHSTORE`)

### Summary

The commit fixes Bug 3 correctly. After parsing a valid `COUNT` integer, `GGroup.geosearch()` now optionally consumes a
following `ANY` token. This makes `COUNT n ANY` valid for both `GEOSEARCH` and `GEOSEARCHSTORE`, while preserving the
existing behavior for invalid count values and stray unknown tokens.

Because Velo currently scans and sorts all candidates before applying `COUNT`, accepting `ANY` as a compatibility
no-op is a reasonable minimal fix.

### Strengths

- The change is tightly scoped to the `COUNT` parsing branch.
- `ANY` is accepted only in the syntactically valid position immediately after `COUNT n`.
- Existing error handling remains intact for missing count, non-integer count, and non-positive count.
- Tests cover both read mode (`GEOSEARCH`) and store mode (`GEOSEARCHSTORE`).

### Concerns

No blocking production-code issues found in the Bug 3 fix.

The only residual concern is test strength. The new tests assert that commands with `COUNT n ANY` are accepted, but
they do not assert that `COUNT` still limits the result size when `ANY` is present.

### Verification

- Ran `./gradlew :test --tests "io.velo.command.GGroupTest"`: passed.
- Checked JaCoCo with `python3 scripts/jacoco_cover.py io.velo.command.GGroup 579 594 --src`: the count parsing branch
  and new `ANY` consumption path are covered.

### Follow-ups

- Pre-commit follow-up: consider strengthening the read-mode test to assert `COUNT 2 ANY` returns exactly two results
  when more than two candidates match.

---

## Review Feedback: Bug 4 Fixed Commit

Reviewer: AI agent 1
Date: 2026-06-01
Commit: `676ba645` (`fix: reject duplicate ASC/DESC in GEOSEARCH and GEOSEARCHSTORE`)

### Summary

The commit fixes Bug 4 correctly. `GGroup.geosearch()` now tracks whether an order option has already been seen and
returns syntax error if another `ASC` or `DESC` appears later in the same request. This rejects both mixed directions
such as `ASC DESC` and repeated directions such as `ASC ASC`.

### Strengths

- The change is narrowly scoped to order parsing.
- The default ascending behavior remains unchanged when no order token is provided.
- Valid single-order requests still work for both `ASC` and `DESC`.
- The focused tests cover the reported mixed-order case and a repeated same-order case.

### Concerns

No blocking production-code issues found in the Bug 4 fix.

The only residual concern is test scope. The new negative tests exercise `GEOSEARCH`; because `GEOSEARCHSTORE` shares
the same parser after destination/source argument rewriting, this is enough for parser behavior, but it does not
directly prove malformed store-mode order input avoids destination mutation.

### Verification

- Ran `./gradlew :test --tests "io.velo.command.GGroupTest"`: passed.
- Checked JaCoCo with `python3 scripts/jacoco_cover.py io.velo.command.GGroup 560 587 --src`: the new order guard has
  full line and branch coverage.

### Follow-ups

- Pre-commit follow-up: optional `GEOSEARCHSTORE ... ASC DESC` negative test with an unchanged destination assertion,
  if you want each store-mode mutation risk covered directly.

---

## Review Feedback: Bug 5 Fixed Commit

Reviewer: AI agent 1
Date: 2026-06-01
Commit: `c733f3fc` (`fix: validate FROMLONLAT bounds and positive BYRADIUS/BYBOX dimensions`)

### Findings

#### Finding 1: `FROMLONLAT NaN` still bypasses validation

Severity: Medium

File:

- `src/main/java/io/velo/command/GGroup.java:631-635`

Code excerpt:

```java
fromLon = Double.parseDouble(new String(dd[i + 1]));
fromLat = Double.parseDouble(new String(dd[i + 2]));
if (fromLon > RedisGeo.GEO_LONG_MAX || fromLon < RedisGeo.GEO_LONG_MIN ||
    fromLat > RedisGeo.GEO_LAT_MAX || fromLat < RedisGeo.GEO_LAT_MIN) {
    return ErrorReply.NOT_FLOAT;
}
```

`Double.parseDouble("NaN")` succeeds, and every comparison with `NaN` is false. That means commands such as
`GEOSEARCH key FROMLONLAT NaN 37 BYRADIUS 100 KM` pass the new bounds check.

Impact:

- The original Bug 5 root cause is not fully fixed for center coordinates.
- With `BYRADIUS`, `RedisGeo.distance(...)` can produce `NaN`; the later filter `if (distance > byRadius)` does not
  exclude `NaN`, so invalid input can return matching rows instead of an error.
- In `GEOSEARCHSTORE`, the same invalid input can still mutate the destination key.

Suggested fix:

Reject non-finite center coordinates before the bounds check:

```java
if (Double.isNaN(fromLon) || Double.isInfinite(fromLon) ||
    Double.isNaN(fromLat) || Double.isInfinite(fromLat) ||
    fromLon > RedisGeo.GEO_LONG_MAX || fromLon < RedisGeo.GEO_LONG_MIN ||
    fromLat > RedisGeo.GEO_LAT_MAX || fromLat < RedisGeo.GEO_LAT_MIN) {
    return ErrorReply.NOT_FLOAT;
}
```

Add tests for `FROMLONLAT NaN 37` and `FROMLONLAT 15 NaN`. A store-mode test should also assert the destination key is
unchanged after invalid input.

### Summary

The commit partially fixes Bug 5. It correctly rejects out-of-range longitude/latitude values, zero or negative
`BYRADIUS`, and zero or negative `BYBOX` dimensions. It also rejects non-finite radius and box dimensions.

However, it does not reject `NaN` for `FROMLONLAT`, so the fix is incomplete.

### Strengths

- The radius and box dimension checks include `Double.isInfinite(...)` and `Double.isNaN(...)`.
- The bounds checks use the same longitude and latitude limits as `GEOADD`.
- Tests cover invalid longitude, invalid latitude, zero and negative radius, zero width, and negative height.

### Verification

- Ran `./gradlew :test --tests "io.velo.command.GGroupTest"`: passed.
- Checked JaCoCo with `python3 scripts/jacoco_cover.py io.velo.command.GGroup 623 697 --src`: the new validation
  branches are covered, but the `FROMLONLAT` non-finite path is absent because the production code does not implement it.

### Follow-ups

- Pre-commit follow-up: fix `FROMLONLAT` to reject `NaN` and infinite coordinates explicitly.
- Pre-commit follow-up: add `NaN` tests for both longitude and latitude, and preferably a store-mode no-mutation test.
