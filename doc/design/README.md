# Velo Design Documentation Index

This directory contains comprehensive design documentation for the Velo key-value storage system.

## Document List

### Core Architecture & Foundation
1. **[01_overall_architecture.md](./01_overall_architecture.md)** - System overview, component relationships, and design philosophy
2. **[02_persist_layer_design.md](./02_persist_layer_design.md)** - Three-tier persistence: WAL, Key Buckets, Chunk Segments
3. **[07_compression_design.md](./07_compression_design.md)** - Zstd compression and dictionary training
4. **[08_configuration_design.md](./08_configuration_design.md)** - Global, slot, and dynamic configuration
5. **[11_multithreading_design.md](./11_multithreading_design.md)** - Threading model and worker architecture
6. **[12_server_bootstrap_design.md](./12_server_bootstrap_design.md)** - Startup sequence and initialization

### Processing Flow
7. **[03_type_system_design.md](./03_type_system_design.md)** - Redis data types implementation (String, Hash, List, Set, ZSet, Geo, etc.)
8. **[04_command_processing_design.md](./04_command_processing_design.md)** - Command parsing and execution (21 command groups A-Z)
9. **[05_protocol_decoding_design.md](./05_protocol_decoding_design.md)** - HTTP/RESP/X-REPL protocol parsing
10. **[06_response_encoding_design.md](./06_response_encoding_design.md)** - RESP2/RESP3/HTTP response encoding

### Advanced Features
11. **[09_replication_design.md](./09_replication_design.md)** - Master-slave replication and failover
12. **[10_acl_security_design.md](./10_acl_security_design.md)** - Access control and user management
13. **[13_metrics_monitoring_design.md](./13_metrics_monitoring_design.md)** - Prometheus metrics and monitoring
14. **[14_rdb_format_design.md](./14_rdb_format_design.md)** - Redis RDB format handling
15. **[15_dynamic_groovy_design.md](./15_dynamic_groovy_design.md)** - Hot-reloadable Groovy commands
16. **[16_cluster_management_design.md](./16_cluster_management_design.md)** - Cluster operations

## Quick Reference

### For New Contributors

1. Start with **01_overall_architecture.md** for system understanding
2. Read **02_persist_layer_design.md** to understand the storage engine
3. Review **08_configuration_design.md** to know how to configure Velo
4. Check **AGENTS.md** (in project root) for development guidelines

### For Feature Development

- **Commands**: Read **04_command_processing_design.md** and **03_type_system_design.md**
- **Storage**: Read **02_persist_layer_design.md** and **07_compression_design.md**
- **Networking**: Read **05_protocol_decoding_design.md** and **06_response_encoding_design.md**
- **Replication**: Read **09_replication_design.md**

### For Operations & Monitoring

- **Configuration**: **08_configuration_design.md**
- **Metrics**: **13_metrics_monitoring_design.md**
- **Observability**: Related documentation in `/doc/metrics/`

## Document Conventions

- **Diagrams**: Mermaid flowcharts/sequence diagrams and ASCII art for data structures
- **Code Examples**: Real implementation snippets from source code
- **Cross-References**: Links to related design documents and source files
- **File Paths**: Provided relative to project root `/home/kerry/ws/velo`

## Contributing

When modifying design documents:
1. Maintain consistency with existing documentation style
2. Update cross-references when adding/modifying sections
3. Verify all Mermaid diagrams render correctly
4. Update this index when adding new documents

## External Documentation

This design documentation complements the existing documentation in `/doc/`:

- **[AGENTS.md](/home/kerry/ws/velo/AGENTS.md)** - Development agent guidelines
- **[doc/hash_buckets/README.md](../doc/hash_buckets/README.md)** - Hash bucket overview
- **[doc/compress/README.md](../doc/compress/README.md)** - Compression basics
- **[doc/multi_threads/README.md](../doc/multi_threads/README.md)** - Threading overview
- **[doc/types/README.md](../doc/types/README.md)** - Type storage modes
- **[doc/repl/README.md](../doc/repl/README.md)** - Replication overview
- **[doc/metrics/README.md](../doc/metrics/README.md)** - Metrics integration
- **[doc/config/README.md](../doc/config/README.md)** - Configuration notes
- **[doc/redis_command_support.md](../doc/redis_command_support.md)** - Command support matrix

---

**Last Updated:** 2025-02-05
**Version:** 1.0
