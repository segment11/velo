# Bug 68-3 Convergent Role Transition Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace partial multi-slot role changes with durable, fenced, restart-resumable promotion and demotion transitions.

**Architecture:** Persist an epoch-based node transition with target role, phase, upstream, per-slot rebase targets, and completed slots. A shared coordinator fences writes before fallible work, prepares slots idempotently, publishes role state only after all preparation succeeds, and leaves failures durably fenced for retry or controller handoff.

**Tech Stack:** Java 21, ActiveJ promises/event loops, Jackson JSON, Groovy/Spock, Gradle, JaCoCo.

---

### Task 1: Durable Transition Record

Create `RoleTransitionState.java` and its Spock test. Test atomic save/load, epochs, immutable per-slot rebase targets, completed-slot persistence, corrupt-file failure, and clear-on-stable behavior. Run the focused test RED, implement JSON persistence through a temporary file plus atomic move, then run GREEN.

### Task 2: Node-wide Fence and Test Seams

Modify `LocalPersist`, `BaseCommand`, and `LeaderSelector`. Test that a node fence rejects writes for every slot and remains active after transition errors. Add injectable master-config lookup and per-slot transition hooks. Enforce a volatile node-wide write fence beside existing readonly checks.

### Task 3: Idempotent Slot Preparation

Modify `OneSlot` with RED tests in `OneSlotTest`. Extract promotion rebase/offset clearing from writable publication. Make demotion fence-first. Add idempotent preparation and commit methods whose role flags publish last.

### Task 4: Shared Convergent Coordinator

Add two-slot RED tests for promotion preparation failure, post-publication failure, demotion failure, repeated invocation, and callback/global ordering. Persist before fencing, prepare all slots, record completion, commit prepared roles, then publish globals. Use `ABORTED` only before publication, `FAILED_FENCED` after publication failure, and forward retry for demotion.

### Task 5: Startup Resume

Add restart tests for `PREPARING`, `COMMITTING`, and `FAILED_FENCED`. Load state and activate the node fence during persistence initialization, then resume after event-loop bindings exist. Require authoritative upstream identity before leaving `FAILED_FENCED`.

### Task 6: Verification and Commit

Run focused `RoleTransitionStateTest`, `OneSlotTest`, `LeaderSelectorTest`, and affected BaseCommand tests. Inspect JaCoCo changed branches, run `:compileJava` and `git diff --check`, append results to the Bug 68 review, and create one Bug 3 fix commit.
