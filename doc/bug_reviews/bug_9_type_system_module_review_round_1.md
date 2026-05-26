# Type System Module Bug Review (Round 1)

Date: 2026-04-21
Author: AI agent 1

## Bug 1: `GEODIST` and `GEOSEARCH WITHDIST` return distances in the wrong unit

**Severity:** High

**Files:** `src/main/java/io/velo/command/GGroup.java:446-478`, `src/main/java/io/velo/command/GGroup.java:714-756`, `src/main/java/io/velo/type/RedisGeo.java:163-181`

```java
// GGroup.java geodist()
var distance = RedisGeo.distance(p0, p1);
return new BulkReply(unit.toMeters(distance));
```

```java
// GGroup.java geosearch()
if (isWithDist) {
    subReplies[ii] = new BulkReply(result.distance());
    ii++;
}
```

```java
// RedisGeo.java
public static double distance(P p0, P p1) {
    ...
    double r = EARTH_RADIUS * c;
    return Math.round(r * Math.pow(10, SCALE_I)) / Math.pow(10, SCALE_I);
}
```

`RedisGeo.distance(...)` returns meters. `GEODIST` then applies `unit.toMeters(...)`, which converts in the wrong direction, so `KM`, `MI`, and `FT` responses are multiplied instead of divided. `GEOSEARCH WITHDIST` has the opposite bug: it always returns the raw meter distance even when the query radius/box used `KM`, `MI`, or `FT`.

Impact:

- `GEODIST key a b KM` returns a value around 1000x too large.
- `GEOSEARCH ... BYRADIUS 10 KM WITHDIST` returns meters instead of kilometers.
- Clients that depend on Redis-compatible geo semantics will silently mis-handle results.

Reference behavior: Redis returns `GEODIST` in the requested unit, and `GEOSEARCH WITHDIST` returns distance in the same unit as the search arguments.

---

## Bug 2: `GEOSEARCH` accepts requests without `FROMMEMBER` or `FROMLONLAT` and searches from `(0,0)`

**Severity:** Medium

**Files:** `src/main/java/io/velo/command/GGroup.java:517-646`

```java
double fromLon = 0d;
double fromLat = 0d;
String fromMember = null;
...
if (byRadius == -1 && byBoxWidth == -1) {
    return ErrorReply.SYNTAX;
}
...
var fromP = new RedisGeo.P(fromLon, fromLat);
```

The parser validates that `BYRADIUS` or `BYBOX` is present, but it never validates that the search center was provided. When neither `FROMMEMBER` nor `FROMLONLAT` appears, the command falls back to the default `(0,0)` center and executes the search.

Impact:

- Invalid `GEOSEARCH` input produces plausible-but-wrong results instead of a syntax error.
- This is a protocol-compatibility bug, not just an internal helper issue.
- Tests can accidentally encode the wrong behavior and hide real client errors.

Reference behavior: Redis requires exactly one center source, `FROMMEMBER` or `FROMLONLAT`.

---

## Bug 3: `GEOADD` does not reject mutually-exclusive flags and does not validate coordinate ranges

**Severity:** High

**Files:** `src/main/java/io/velo/command/GGroup.java:366-444`, `src/main/java/io/velo/type/RedisGeo.java:237-263`

```java
boolean isNx = false;
boolean isXx = false;
...
if (arg.equalsIgnoreCase("nx")) {
    isNx = true;
} else if (arg.equalsIgnoreCase("xx")) {
    isXx = true;
}
```

```java
try {
    var lon = Double.parseDouble(new String(data[i]));
    var lat = Double.parseDouble(new String(data[i + 1]));
    var member = new String(data[i + 2]);
    itemList.add(new GeoItem(lon, lat, member));
    i += 2;
} catch (NumberFormatException e) {
    return ErrorReply.NOT_FLOAT;
}
```

```java
static long geohashEncode(...) {
    if (lon > GEO_LONG_MAX || lon < GEO_LONG_MIN || lat > GEO_LAT_MAX || lat < GEO_LAT_MIN) {
        return 0;
    }
```

Two separate Redis-compatibility checks are missing:

1. `NX` and `XX` are accepted together. The later write path effectively lets `NX` win because the update logic uses `if (isNx) ... else if (isXx) ...`, so the command executes instead of failing.
2. Parsed longitude and latitude are never validated before storage. The low-level geohash helper knows the allowed bounds, but `geoadd()` never calls that validation and stores out-of-range coordinates directly.

Impact:

- `GEOADD key NX XX ...` succeeds instead of returning a syntax error.
- Invalid coordinates like longitude `181` or latitude `90` can be persisted.
- Stored invalid points can later produce inconsistent behavior across `GEODIST`, `GEOHASH`, and `GEOSEARCH`.

Reference behavior: Redis treats `NX` and `XX` as mutually exclusive and rejects out-of-range coordinates.

---

## Bug 4: `BF.INSERT` and `BF.RESERVE` leak raw Java parse failures instead of Redis errors

**Severity:** Medium

**Files:** `src/main/java/io/velo/command/BGroup.java:511-615`, `src/main/java/io/velo/command/BGroup.java:686-729`, `src/main/java/io/velo/RequestHandler.java:514-530`

```java
// BGroup.java bfInsert()
initCapacity = Integer.parseInt(new String(data[i + 1]));
...
initFpp = Double.parseDouble(new String(data[i + 1]));
...
initExpansion = Byte.parseByte(new String(data[i + 1]));
```

```java
// BGroup.java bfReserve()
var initFpp = Double.parseDouble(new String(data[2]));
...
var initCapacity = Integer.parseInt(new String(data[3]));
```

```java
// RequestHandler.java
} catch (Exception e) {
    log.error("Request handle error.", e);
    return new ErrorReply(e.getMessage());
}
```

Unlike most command parsers in the codebase, these Bloom filter commands do not catch `NumberFormatException`. Bad numeric input therefore falls through to the generic request handler and is returned as a raw Java exception message rather than `NOT_INTEGER`, `NOT_FLOAT`, or another protocol-level reply.

Impact:

- `BF.RESERVE key x 100` or malformed `BF.INSERT ... CAPACITY nope` does not behave like a normal Redis command parser.
- Error text depends on JVM parsing messages instead of stable server replies.
- Clients that depend on exact Redis-style error handling will see inconsistent behavior.

---

## Bug 5: `BF.INFO SIZE` is hardcoded to `0`

**Severity:** Low

**Files:** `src/main/java/io/velo/type/RedisBF.java:173-181`, `src/main/java/io/velo/command/BGroup.java:456-509`

```java
public int memoryAllocatedEstimate() {
    // todo
    return 0;
}
```

```java
case "size":
    replies[0] = new IntegerReply(redisBF.memoryAllocatedEstimate());
    break;
```

The command path exposes `BF.INFO SIZE`, but the underlying implementation is still a stub and always returns `0`.

Impact:

- `BF.INFO key` and `BF.INFO key SIZE` return misleading metadata for every filter.
- This is an observable behavior bug in the type-system module, not just a missing optimization.

---

---

## AI Agent 2 Review Notes (Round 1)

Reviewed: 2026-04-21
Reviewer: AI Agent 2

### Bug 1: `GEODIST` and `GEOSEARCH WITHDIST` return distances in the wrong unit
**Status: CONFIRMED**

- `RedisGeo.distance()` JavaDoc explicitly states "Returns the distance between the two points in meters" (line 161-162). The return value is always in meters.
- In `geodist()` at GGroup.java:477, `unit.toMeters(distance)` is called. The `Unit` enum's `toMeters()` multiplies by the conversion factor (KM: ×1000, MI: ×1609.34, FT: ×0.3048). Since `distance` is already in meters, this produces wrong results (KM output is 1000× too large).
- The `Unit` enum (RedisGeo.java:25-78) only has `toMeters()` (converts TO meters). There is no `fromMeters()` method to convert FROM meters TO the target unit.
- Fix for `GEODIST`: Add `Unit.fromMeters(double meters)` method and use it, or divide by the conversion factor.
- For `GEOSEARCH WITHDIST` (GGroup.java:738-740): `result.distance()` is returned directly without unit conversion. While `byRadius`/`byBoxWidth`/`byBoxHeight` are converted TO meters for filtering (lines 588, 609-610), the output distance is never converted back to the user's unit. The `byRadiusUnit` variable is available and should be used to convert the result.
- **Refinement**: `byRadiusUnit` is a local variable in the parsing loop and is not preserved for use at output time. The fix requires storing the user's unit for use in the result formatting.

### Bug 2: `GEOSEARCH` accepts requests without `FROMMEMBER` or `FROMLONLAT`
**Status: CONFIRMED**

- GGroup.java:520-522 initializes `fromLon = 0d; fromLat = 0d; fromMember = null;`
- Lines 625-627 only validate that `BYRADIUS` or `BYBOX` is present — not that a center source was provided.
- When neither `FROMMEMBER` nor `FROMLONLAT` is in the command, `fromMember` stays null and `fromP` is constructed from `(0,0)` at line 645.
- The search then proceeds with the Atlantic Ocean origin point, producing plausible-but-wrong results.
- Fix: Add validation after the parsing loop to ensure `fromMember != null || (fromLon != 0d || fromLat != 0d)` is satisfied, or add an explicit flag tracking whether a center was provided.

### Bug 3: `GEOADD` does not reject mutually-exclusive flags and does not validate coordinate ranges
**Status: CONFIRMED (both sub-issues)**

**Sub-issue 1 (NX/XX mutual exclusion):**
- GGroup.java:373-384 sets `isNx` and `isXx` independently with no mutual-exclusion check.
- Lines 415-425 use `if (isNx) ... else if (isXx) ...`, so when both are true, `NX` behavior executes silently.
- Fix: Add `if (isNx && isXx) return ErrorReply.SYNTAX;` after parsing.

**Sub-issue 2 (coordinate range validation):**
- GGroup.java:390-398 parses lon/lat with `Double.parseDouble()` but never validates bounds.
- `RedisGeo` has `GEO_LONG_MAX = 180.0`, `GEO_LONG_MIN = -180.0`, `GEO_LAT_MAX = 85.05112877980659`, `GEO_LAT_MIN = -85.05112877980659` (lines 237-240).
- `geohashEncode()` (line 257) checks these bounds but is never called by `geoadd()`.
- Fix: Add explicit lon/lat range validation in `geoadd()` using the same constants, before constructing `GeoItem`.

### Bug 4: `BF.INSERT` and `BF.RESERVE` leak raw Java parse failures instead of Redis errors
**Status: CONFIRMED**

- `bfInsert()` at lines 545 (`Integer.parseInt`), 553 (`Double.parseDouble`), 564 (`Byte.parseByte`) — all unvalidated.
- `bfReserve()` at lines 694 (`Double.parseDouble`) and 699 (`Integer.parseInt`) — unvalidated.
- These throw `NumberFormatException` which is caught by `RequestHandler.java:527-529` and returned as `new ErrorReply(e.getMessage())` — raw JVM error text.
- Fix: Wrap these parses in try-catch blocks returning `ErrorReply.NOT_INTEGER` / `ErrorReply.NOT_FLOAT` as appropriate.

### Bug 5: `BF.INFO SIZE` is hardcoded to `0`
**Status: CONFIRMED**

- `RedisBF.memoryAllocatedEstimate()` (lines 178-180) is literally `// todo return 0;`
- The `BF.INFO` command path at BGroup.java:183 passes this directly to `IntegerReply`.
- The JavaDoc on line 176 even says "not yet implemented" — this is an intentional stub, not a bug that's hidden.
- Fix: Implement the memory estimation or change `BF.INFO SIZE` to return an appropriate error indicating the feature is not implemented.

### Summary
All 5 bugs confirmed. All are valid Redis-compatibility issues. The bugs in the GEO module are particularly severe since they produce plausible wrong results rather than errors, making them hard for clients to detect.

---

## Review Feedback (Round 2 - Fix Verification)

Reviewed: 2026-04-21
Reviewer: AI Agent 2

### Summary of Fixes

**Bug 1 - GEODIST/GEOSEARCH WITHDIST wrong unit conversion:**
- Added `Unit.fromMeters(double meters)` method to `RedisGeo.Unit` enum
- Changed `geodist()` from `unit.toMeters(distance)` to `unit.fromMeters(distance)`
- Added `outputUnit` variable in `geosearch()` to track user's unit, used to convert `result.distance()` at output

**Bug 2 - GEOSEARCH missing center validation:**
- Added `hasCenterSource` boolean flag set to `true` when `FROMMEMBER` or `FROMLONLAT` is parsed
- Added validation `if (!hasCenterSource) return ErrorReply.SYNTAX;` after parsing loop

**Bug 3 - GEOADD NX/XX + coordinate validation:**
- Added `if (isNx && isXx) return ErrorReply.SYNTAX;` after itemList check
- Added coordinate range validation before constructing `GeoItem`: checks against `RedisGeo.GEO_LONG_MAX/MIN` and `GEO_LAT_MAX/MIN`
- Made `GEO_LONG_MAX`, `GEO_LONG_MIN`, `GEO_LAT_MAX`, `GEO_LAT_MIN` `public static final` in `RedisGeo` to allow access from `GGroup`

**Bug 4 - BF.INSERT/BF.RESERVE parse failure leaks:**
- Wrapped `Integer.parseInt`, `Double.parseDouble`, and `Byte.parseByte` calls in try-catch blocks
- Returns `ErrorReply.NOT_INTEGER` or `ErrorReply.NOT_FLOAT` as appropriate

**Bug 5 - BF.INFO SIZE hardcoded to 0:**
- Implemented `memoryAllocatedEstimate()` using Bloom filter size formula: `m = -n * ln(p) / (ln(2)^2)` bits per filter, summed and converted to bytes

### Test Results
- ✅ BGroupTest: All passed
- ✅ RedisBFTest: All passed
- ✅ RedisGeoTest: All passed
- ❌ GGroupTest: 2 failures (expected - tests were encoding wrong behavior that fixes correct)
  - `test geoadd` line 364: `geoadd a nx xx ...` now correctly returns SYNTAX error
  - `test geosearch and geosearchstore` line 636: `geosearch` without center now correctly returns SYNTAX error

### Concerns
- The test failures in GGroupTest confirm the fixes are working correctly. The tests at lines 362 (`geoadd a nx xx ch 1.0 2.0 m0`) and 634 (`geosearch xxx byradius 100 km bybox 1 1 km`) were asserting buggy behavior that has been corrected.
- `GEO_LONG_*` and `GEO_LAT_*` constants in `RedisGeo` were changed from `private` to `public static final`. This is a minor API exposure but necessary for validation in `GGroup`.

### Pre-commit Checklist
- [x] Update GGroupTest to expect SYNTAX error for `geoadd nx xx` combination
- [x] Update GGroupTest to expect SYNTAX error for `geosearch` without center source
- [x] Verify JaCoCo coverage for modified code paths

### Post-commit Follow-ups
- [x] Run full test suite after updating tests encoding wrong behavior
- [ ] Consider adding integration tests for Redis compatibility on geo commands

---

## Review Feedback (Round 3 - Follow-up Commit Review)

Reviewed: 2026-04-21
Reviewer: AI Agent 2
Commit reviewed: `75d197e` (`fix: geo and bloomfilter type system bugs`)

### Findings

#### Finding 1 — Medium: `GEOSEARCH` still accepts both `FROMMEMBER` and `FROMLONLAT`

- `src/main/java/io/velo/command/GGroup.java:565-583` sets parser state for both center forms independently.
- `src/main/java/io/velo/command/GGroup.java:641-665` only checks that some center source exists, then lets `FROMMEMBER` overwrite the previously parsed lon/lat values.
- `src/test/groovy/io/velo/command/GGroupTest.groovy:573-584` now exercises a request that includes both center forms and expects success, which locks in the invalid behavior.

```java
} else if ("fromlonlat".equalsIgnoreCase(arg)) {
    ...
    hasCenterSource = true;
} else if ("frommember".equalsIgnoreCase(arg)) {
    ...
    hasCenterSource = true;
}
...
if (fromMember != null) {
    var p = rg.get(fromMember);
    ...
    fromLon = p.lon();
    fromLat = p.lat();
}
```

Redis defines the center clause as a mutually-exclusive choice: `FROMMEMBER member | FROMLONLAT longitude latitude`. The current parser accepts both and silently prefers `FROMMEMBER`, so invalid syntax becomes a successful query with ignored arguments.

#### Finding 2 — Medium: `GEOSEARCH` still accepts both `BYRADIUS` and `BYBOX`

- `src/main/java/io/velo/command/GGroup.java:584-603` parses and stores `BYRADIUS`.
- `src/main/java/io/velo/command/GGroup.java:604-625` also parses and stores `BYBOX` in the same request.
- `src/main/java/io/velo/command/GGroup.java:676-685` applies both predicates together, effectively turning invalid syntax into an intersection query.
- `src/main/java/io/velo/command/GGroup.java:645-649` and `src/main/java/io/velo/command/GGroup.java:760-761` select the output distance unit from only one shape branch, even when both were specified.

```java
} else if ("byradius".equalsIgnoreCase(arg)) {
    ...
    byRadius = byRadiusUnit.toMeters(byRadius);
} else if ("bybox".equalsIgnoreCase(arg)) {
    ...
    byBoxWidth = byBoxUnit.toMeters(byBoxWidth);
    byBoxHeight = byBoxUnit.toMeters(byBoxHeight);
}
...
if (byRadius != -1) {
    if (distance > byRadius) {
        continue;
    }
}

if (byBoxWidth != -1) {
    if (!RedisGeo.isWithinBox(p, fromLon, fromLat, byBoxWidth, byBoxHeight)) {
        continue;
    }
}
```

Redis defines the shape clause as a mutually-exclusive choice: `BYRADIUS ... | BYBOX ...`. The current implementation accepts both and evaluates both filters, so invalid syntax returns data instead of a syntax error.

### Verification

- Reviewed commit diff for `75d197e00422a50ab3bf308931339667955c0b49`
- Ran: `./gradlew :test --tests 'io.velo.command.GGroupTest' --tests 'io.velo.command.BGroupTest' --tests 'io.velo.type.RedisGeoTest'`
- Result: pass

### Summary

The commit correctly fixes the previously documented GEO unit conversion and Bloom filter parse-handling bugs, but the `GEOSEARCH` parser still does not enforce Redis `oneof` exclusivity for:

- center source: `FROMMEMBER` vs `FROMLONLAT`
- shape filter: `BYRADIUS` vs `BYBOX`

Those two cases remain open protocol-compatibility issues.

---

## Review Feedback (Round 4 - Fix Verification for Findings 1 & 2)

Reviewed: 2026-04-21
Commit: `73ec911` (`fix: GEOSEARCH enforce mutual exclusion for center and shape options`)

### Summary of Fixes

**Finding 1 - GEOSEARCH accepts both FROMMEMBER and FROMLONLAT:**
- Added `hasFromLonLat` boolean flag
- Added mutual exclusion check: `if (hasFromLonLat || fromMember != null) return ErrorReply.SYNTAX;` in both FROMLONLAT and FROMMEMBER parsing branches
- Tests updated to expect SYNTAX when both center forms are provided

**Finding 2 - GEOSEARCH accepts both BYRADIUS and BYBOX:**
- Added `hasByRadius` and `hasByBox` boolean flags
- Added mutual exclusion check: `if (hasByRadius || hasByBox) return ErrorReply.SYNTAX;` in both BYRADIUS and BYBOX parsing branches
- Tests updated to expect SYNTAX when both shape forms are provided

### Test Results
- `./gradlew :test --tests 'io.velo.command.GGroupTest' --tests 'io.velo.command.BGroupTest' --tests 'io.velo.type.RedisGeoTest' --tests 'io.velo.type.RedisBFTest'`
- Result: all pass
