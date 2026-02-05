# Velo Cluster Management Design

## Overview

Velo cluster management通过**Groovy脚本**实现，支持slot迁移、failover等集群操作。

## Cluster架构

```
┌─────────────────────────────────────────────┐
│                    Cluster                │
│  ├─> Node A: Slot 0-31                        │
│  ├─> Node B: Slot 32-63                      │
│  ├─> Node C: Slot 64-95                      │
│  └─> Node D: Slot 96-127                     │
└─────────────────────────────────────────────┘

Each Node:
  - 独立的Redis兼容实例
  - 自有的slot集合
  - 与ZooKeeper协调
```

## Slot迁移

### 迁移流程

```
Slot迁移状态机:

┌──────┐
│FREE │
└──┬───┘
   │
   ▼ START迁移
┌──────┐
│IMPORT│ ← 从源节点导入数据
└──┬───┘
   │
   ▼ 数据导入完成
┌──────┐
│SERVING│ ← 接受读写
└──────┘
```

### 迁移实现

```groovy
@CompileStatic
class ClusterxCommandHandle {
    static Reply handleMigration(OneSlot slot, ExtArgs args) {
        // 1. 检查当前状态
        String state = slot.getState();

        // 2. 开始迁移
        if ("START".equals(args.get("action"))) {
            startMigration(slot);
        }

        // 3. 迁移数据
        if ("SYNC".equals(args.get("action"))) {
            syncDataFromSource(slot, sourceNode);
        }

        // 4. 完成迁移
        if ("COMPLETE".equals(args.get("action"))) {
            completeMigration(slot);
        }

        return OKReply.INSTANCE;
    }
}
```

### 数据同步

```java
class ClusterNode {
    private final String nodeId;
    private final Map<Short, OneSlot> slots;

    public void syncSlot(short slotIndex, String sourceNodeId) {
        OneSlot targetSlot = slots.get(slotIndex);
        OneSlot sourceSlot = connectToNode(sourceNodeId, slotIndex);

        // 1. Fetch existing data
        List<byte[]> keys = sourceSlot.scan();

        // 2. Write to target
        for (byte[] keyBytes : keys) {
            CompressedValue cv = sourceSlot.getCV(keyBytes);
            if (cv != null) {
                targetSlot.setCV(keyBytes, cv);
            }
        }

        // 3. Update slot state
        targetSlot.setState("SERVING");
    }
}
```

## Failover机制

### 自动Failover

```
ZooKeeper协调:

┌─────────────────────────────────────────┐
│          ZooKeeper Cluster              │
└─────────────────────────────────────────┘
         │
         ├─> Watch Leader Node Latch
         │
         ├──> 当Leader失败:
         ├─>     1. Latch释放
         ├─>     2. 剩余节点竞争Latch
         ├─>     3. 新Leader当选
         └─>     4. 更新客户端配置
```

### Failover状态

```groovy
class ClusterxCommandHandle {
    static Reply handleFailOver(ExtArgs args) {
        String nodeId = args.get("nodeId");

        // 1. 检查节点状态
        boolean isAlive = checkNodeAlive(nodeId);

        // 2. 如果down,触发failover
        if (!isAlive) {
            triggerFailover(nodeId);
        }

        return OKReply.INSTANCE;
    }

    private static void triggerFailover(String failedNodeId) {
        // 通过ZooKeeper重新选举
        String newLeader = electNewLeader();

        // 更新所有节点的路由表
        updateRoutingTable(failedNodeId, newLeader);

        // 通知客户端重新连接
        notifyClientsClusterChange();
    }
}
```

## Cluster命令

```
CLUSTERX <action> <params>

Actions:
  - list → 列出所有节点和slot分布
  - info <node> → 查看节点信息
  - migrate <slot> <from> <to> → 迁移slot
  - failover <node> → 触发failover
  - addnode <node> → 添加节点
  - delnode <node> → 移除节点
```

## 相关文档

- [Replication Design](./09_replication_design.md) - 复制协议
- Existing: [doc/cluster/](../doc/cluster/)

## 关键源文件

- `src/main/groovy/io/velo/repl/cluster/` - 集群管理
- `dyn/src/io/velo/command/ClusterxCommand.groovy` - 集群命令

---

**Version:** 1.0
**Last Updated:** 2025-02-05
