# Bug <N> — <Module> Review (Round <M>)

**Module:** <io.velo.package / classes>
**Round:** <M>
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** <discovery complete — N findings, none verified yet>
**Review branch:** `review/<module>`
**Reviewed commit:** <40-char sha>

> Delta declaration (rounds M>1 only): prior round doc `bug_<X>_<module>_review_round_<M-1>.md` — <a> fixed / <b> won't fix / <c> open. Not re-reported: <list or "none">.

## Scope

Files read (method + line ranges):
- `<File>.java — <method>() (<start>-<end>)`

Reference docs (ground truth):
- <URL / Redis 7.2 doc / design doc path>

## Finding 1 (Bug <N>.1): <one-line title>

**Severity:** High | Medium | Low
**Class:** Correctness | Concurrency | Security | Redis protocol compatibility | Edge case
**Status:** `<OPEN — not verified>`

### Cited files / lines
```java
// <File>.java:<lines> — excerpt
```

### Root cause
<why this is wrong>

### Trigger / impact
<conditions + blast radius, contrasted with Redis semantics>

### Suggested fix direction
<approach + constraints>

## Notes / non-findings

- <checked and correct — kept so later rounds don't re-investigate>

## Reviewer checklist (for AI agent 2)

- Finding 1: <reproduction recipe — data to construct, assertion to make>

## Review Feedback (AI agent 2, round <M>)

**Method:** <Spock temp spec | live Redis 7.2 differential | JDK repro | static inspection>

| Finding | Verdict | Evidence |
|---|---|---|
| N.1 | CONFIRMED (reproduced) | <pasted output/summary> |

### Additional observations
### Recommended fix order (for the fix agent)

## Fix Log (AI agent 1, round <M> fix phase)

| Bug | Failing test evidence | Fix commit | Focused test | JaCoCo |
|---|---|---|---|---|
| N.1 | <test name + red output> | `<short sha>` `fix: ...` | green | <lines/branches covered> |

## Re-review (AI agent 2)

<per-commit verification, repeat as needed>

## Residual Risk

<what remains unfixed / won't fix and why>

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| N.1 | | | | | |
