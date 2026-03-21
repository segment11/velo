# Velo Dynamic Groovy Design

## Overview

The codebase contains Groovy loading infrastructure, but the checked-in Groovy command surface is much
smaller than older design docs claimed.

The current Java integration points are:

- [`CachedGroovyClassLoader`](/home/kerry/ws/velo/src/main/java/io/velo/dyn/CachedGroovyClassLoader.java)
- [`RefreshLoader`](/home/kerry/ws/velo/src/main/java/io/velo/dyn/RefreshLoader.java)
- bootstrap wiring in [`MultiWorkerServer.refreshLoader()`](/home/kerry/ws/velo/src/main/java/io/velo/MultiWorkerServer.java)

## Loader Behavior

`CachedGroovyClassLoader` is a singleton wrapper around `GroovyClassLoader`.
Important current behaviors:

- initializes one Groovy class loader
- forces UTF-8 source encoding
- adds `@CompileStatic` through an AST transformation customizer
- caches parsed classes
- invalidates cached classes when the source file timestamp changes

This is real hot-reload infrastructure, not pseudocode.

## Refresh Loader

`MultiWorkerServer` creates a [`RefreshLoader`](/home/kerry/ws/velo/src/main/java/io/velo/dyn/RefreshLoader.java)
from the Groovy class loader and points it at `Utils.projectPath("/dyn/src/io/velo")`.

That means the runtime is prepared to load Groovy sources from that tree when present.

## What Is Actually In The Repository

The current checkout contains Groovy files such as:

- `dyn/ctrl/FailoverManagerCtrl.groovy`
- `dyn/src/Test.groovy`
- `dyn/src/ext/tools/DoMockData.groovy`
- `dyn/src/ext/tools/DoWarmUpKeyBuckets.groovy`

It does not contain the large `dyn/src/io/velo/command/*` tree described in older versions of this doc.

## Practical Interpretation

Today the design should be read as:

- Groovy hot reload infrastructure exists and is wired into bootstrap.
- The repository currently includes utility/control Groovy scripts rather than a broad dynamic command suite.
- Any documentation that claims a populated dynamic command package should be treated as aspirational, not current.

## Related Documents

- [Server Bootstrap](/home/kerry/ws/velo/doc/design/12_server_bootstrap_design.md)
- [Cluster Management](/home/kerry/ws/velo/doc/design/16_cluster_management_design.md)
