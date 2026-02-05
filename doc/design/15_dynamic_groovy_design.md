# Velo Dynamic Groovy Design

## Overview

Velo支持**Groovy脚本热加载**,无需重启服务器即可添加/修改命令。

## Hot-Reload Architecture

```
dyn/src/ext/tools/          # Maintenance tools
  ├── DoWarmUpKeyBuckets.groovy
  └── DoMockData.groovy

dyn/src/io/velo/command/   # 扩展命令
  ├── ManageCommand.groovy    # 管理命令
  ├── ExtendCommand.groovy    # 扩展命令
  ├── ConfigCommand.groovy    # 配置命令
  ├── InfoCommand.groovy      # 信息命令
  └── CommandCommand.groovy   # 命令管理

dyn/src/io/velo/script/    # 命令处理逻辑
  ├── ManageCommandHandle.groovy
  ├── ExtendCommandHandle.groovy
  └── ClusterxCommandHandle.groovy
```

## CachedGroovyClassLoader

```java
class CachedGroovyClassLoader {
    private final Map<String, GroovyClassLoader> loaderCache;

    public Class<?> load(String className, String filePath, long lastModified) {
        // 1. Check if file was modified
        var entry = loaderCache.get(className);
        if (entry != null && entry.lastModified == lastModified) {
            return entry.cachedClass;
    }

        // 2. Create new class loader
        GroovyClassLoader gcl = new GroovyClassLoader(getClass().getClassLoader());

        // 3. Configure compilation
        CompilerConfiguration config = new CompilerConfiguration();
        config.addCompilationCustomizers(new ASTTransformationCustomizer(ASTTransformation.CompileMode.INSTRUCTION_ONLY));

        gcl.setConfig(config);

        // 4. Parse Groovy source
        File scriptFile = new File(filePath);
        assert scriptFile.exists();

        GroovyCodeSource gcs = new GroovyCodeSource(scriptFile);
        Class<?> scriptClass = gcl.parseClass(gcs);

        // 5. Cache result
        loaderCache.put(className, new CacheEntry(scriptClass, lastModified));

        return scriptClass;
    }
}
```

## @CompileStatic 性能要求

**强制要求**: 所有Groovy脚本必须使用@CompileStatic注解

```groovy
import groovy.transform.CompileStatic
import groovy.transform.TypeChecked

@CompileStatic
@TypeChecked
class MyCommand {
    static String handle(byte[]... args) {
        // 强类型Groovy码
        if (args.length < 2) {
            return "ERROR"
        }
        return "OK"
    }
}
```

**原因**:
- 类型检查确保性能
- 避免动态特性开销
- 接近Java性能

## ExtendCommand Pattern

```groovy
@CompileStatic
@TypeChecked
class ExtendCommand {
    static String parseSlots(OneSlot oneSlot, byte[][] data) {
        // 实现parseSlots逻辑
        return "OK"
    }

    static Reply handle(ExtArgs extArgs) {
        // 实现handle逻辑
        return OKReply.INSTANCE
    }
}
```

## 动态加载流程

```
1. 检测文件修改
   ├─> CachedGroovyClassLoader.load()
   ├─> GroovyClassLoader.cache = false
   └─> 重新解析Groovy源码

2. 编译和加载
   ├─> AST转换为Java字节码
   ├─> 验证@CompileStatic注解
   └─> 返回Class对象

3. 注册命令
   ├─> 缓存类对象
   ├─> 创建命令实例
   └─> 集成到扩展命令系统
```

## 文件监控

```java
public class GroovyReloader {
    private final Map<String, Long> fileTimestamps = new HashMap<>();

    public void checkAndReload() {
        dyn/src/io/velo/command/目录下的.groovy文件

        for (File file : listGroovyFiles()) {
            long currentModified = file.lastModified();

            if (fileTimestamps.get(file.getName()) != currentModified) {
                reloadGroovyFile(file);
                fileTimestamps.put(file.getName(), currentModified);
            }
        }
    }

    private void reloadGroovyFile(File file) {
        String className = extractClassName(file);
        String filePath = file.getAbsolutePath();
        long lastModified = file.lastModified();

        try {
            Class<?> clazz = cachedLoader.load(className, filePath, lastModified);

            // 验证@CompileStatic
            if (!clazz.isAnnotationPresent(CompileStatic.class)) {
                throw new RuntimeException("Groovy script must have @CompileStatic");
            }

            // 重新注册命令
            registerCommand(clazz);

        } catch (Exception e) {
            log.error("Failed to reload Groovy file: " + file, e);
        }
    }
}
```

## 相关文档

- [Command Processing](./04_command_processing_design.md) - 命令集成
- Existing: [doc/extend_commands/README.md](../doc/extend_commands/README.md)

## 关键源文件

- `dyn/src/io/velo/command/*Command.groovy` - 扩展命令
- `dyn/src/io/velo/script/*Handle.groovy` - 处理逻辑

---

**Version:** 1.0
**Last Updated:** 2025-02-05
