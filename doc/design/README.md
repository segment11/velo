# Velo Design Documentation Index

This directory documents the design that is present in the checked-in code, not historical or planned structures.

## Recommended Reading Order

1. [01_overall_architecture.md](/home/kerry/ws/velo/doc/design/01_overall_architecture.md)
2. [02_persist_layer_design.md](/home/kerry/ws/velo/doc/design/02_persist_layer_design.md)
3. [04_command_processing_design.md](/home/kerry/ws/velo/doc/design/04_command_processing_design.md)
4. [05_protocol_decoding_design.md](/home/kerry/ws/velo/doc/design/05_protocol_decoding_design.md)
5. [09_replication_design.md](/home/kerry/ws/velo/doc/design/09_replication_design.md)
6. [12_server_bootstrap_design.md](/home/kerry/ws/velo/doc/design/12_server_bootstrap_design.md)

## Notes About Current Scope

- Command processing is organized into 26 A-Z group classes.
- Replication and cluster leadership are primarily Java implementations under `io.velo.repl`.
- Dynamic Groovy support exists, but the checked-in Groovy source tree is limited compared with earlier docs.
- The repository does not currently contain a populated `src/main/groovy` application module.

## Document List

- [01_overall_architecture.md](/home/kerry/ws/velo/doc/design/01_overall_architecture.md)
- [02_persist_layer_design.md](/home/kerry/ws/velo/doc/design/02_persist_layer_design.md)
- [03_type_system_design.md](/home/kerry/ws/velo/doc/design/03_type_system_design.md)
- [04_command_processing_design.md](/home/kerry/ws/velo/doc/design/04_command_processing_design.md)
- [05_protocol_decoding_design.md](/home/kerry/ws/velo/doc/design/05_protocol_decoding_design.md)
- [06_response_encoding_design.md](/home/kerry/ws/velo/doc/design/06_response_encoding_design.md)
- [07_compression_design.md](/home/kerry/ws/velo/doc/design/07_compression_design.md)
- [08_configuration_design.md](/home/kerry/ws/velo/doc/design/08_configuration_design.md)
- [09_replication_design.md](/home/kerry/ws/velo/doc/design/09_replication_design.md)
- [10_acl_security_design.md](/home/kerry/ws/velo/doc/design/10_acl_security_design.md)
- [11_multithreading_design.md](/home/kerry/ws/velo/doc/design/11_multithreading_design.md)
- [12_server_bootstrap_design.md](/home/kerry/ws/velo/doc/design/12_server_bootstrap_design.md)
- [13_metrics_monitoring_design.md](/home/kerry/ws/velo/doc/design/13_metrics_monitoring_design.md)
- [14_rdb_format_design.md](/home/kerry/ws/velo/doc/design/14_rdb_format_design.md)
- [15_dynamic_groovy_design.md](/home/kerry/ws/velo/doc/design/15_dynamic_groovy_design.md)
- [16_cluster_management_design.md](/home/kerry/ws/velo/doc/design/16_cluster_management_design.md)

## Related Non-Design Docs

- [AGENTS.md](/home/kerry/ws/velo/AGENTS.md)
- [doc/haproxy/README.md](/home/kerry/ws/velo/doc/haproxy/README.md)
- [doc/performance/README.md](/home/kerry/ws/velo/doc/performance/README.md)
- [doc/redis_command_support.md](/home/kerry/ws/velo/doc/redis_command_support.md)
