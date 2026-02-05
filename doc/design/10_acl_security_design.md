# Velo ACL Security Design

## Overview

Velo implements **Redis 7+ compatible ACL** (Access Control List) for fine-grained user and permission management.

## User Model

### AclUsers Structure

```java
public class AclUsers {
    public static AclUsers getInstance() {
        return instance;
    }

    private final Map<String, AclUser> users;  // username -> User

    public static class AclUser {
        String user;
        boolean isOn;
        List<Password> passwords;
        List<RCmd> rCmdList;           // Allowed commands
        List<RCmd> rCmdDisallowList;   // Disallowed commands
        List<RKey> rKeyList;           // Key permissions
        List<RPubSub> rPubSubList;     // Pub/Sub channels
    }

    @ThreadNeedLocal("net")
    private static AclUsers[] perWorkerAclUsers;
}
```

### User Structure

```
User Configuration Example:
user default on nopass +@all ~* &*

Fields:
  user: "default"
  isOn: true (enabled)
  passwords: [Password(type=nopass, value=null)]
  rCmdList: [+@all]         (Allow all commands)
  rCmdDisallowList: []
  rKeyList: [~*]            (Access all keys)
  rPubSubList: [&*]          (Access all channels)
```

### Password Management

```java
class Password {
    String type;  // "nopass" | "plain" | "sha256"
    String value; // null for nopass, hash for sha256

    static Password nopass() {
        return new Password("nopass", null);
    }

    static Password plain(String password) {
        return new Password("plain", password);
    }

    static Password sha256(String password) {
        String hash = DigestUtils.sha256Hex(password);
        return new Password("sha256", hash);
    }

    boolean matches(String inputPassword) {
        if ("nopass".equals(type)) {
            return inputPassword == null || inputPassword.isEmpty();
        }
        if ("plain".equals(type)) {
            return value.equals(inputPassword);
        }
        if ("sha256".equals(type)) {
            return value.equals(DigestUtils.sha256Hex(inputPassword));
        }
        return false;
    }
}
```

## Command Permissions

### RCmd Types

```java
public class RCmd {
    enum Type {
        CMD,           // Specific command: +get
        CMD_WITH_ARG,  // Command with subcommand: +config|get
        CATEGORY,      // Command category: +@read
        ALL            // All commands: +*
    }

    Type type;
    boolean isAllow;  // + for allow, - for deny
    String cmdName;   // Command name or category
}
```

### Permission Patterns

```
Allow Rules:
  +@read           - Allow all read commands
  +@write          - Allow all write commands
  +@admin          - Allow administrative commands
  +get             - Allow GET command
  +config|get      - Allow CONFIG GET subcommand  
  +*               - Allow all commands

Deny Rules:
  -@dangerous      - Deny dangerous commands
  -flushall        - Deny FLUSHALL
  -@all            - Deny all commands
```

### Command Categories

| Category | Commands |
|----------|----------|
| all | All commands |
| admin | CONFIG, FLUSHALL, SAVE, BGSAVE |
| bitmap | BITCOUNT, BITOP, BITPOS, etc. |
| connection | AUTH, PING, QUIT, CLIENT commands |
| geo | GEOADD, GEODIST, GEOSEARCH, etc. |
| hash | HGET, HSET, HMGET, etc. |
| hyperloglog | PFADD, PFCOUNT, PFMERGE |
| key | DEL, EXISTS, KEYS, SCAN, etc. |
| list | LPOP, RPUSH, LRANGE, etc. |
| pubsub | PUBLISH, SUBSCRIBE, PSUBSCRIBE |
| read | Commands that only read |
| scripting | EVAL, SCRIPT LOAD |
| set | SADD, SMEMBERS, SINTER, etc. |
| sortedset | ZADD, ZRANGE, ZSCORE, etc. |
| stream | XADD, XRANGE, etc. |
| string | GET, SET, MGET, MSET, etc. |
| transaction | MULTI, EXEC, DISCARD |
| write | Commands that modify data |

### Permission Check Logic

```java
boolean checkCmdAndKey(
    AclUser user, 
    String command, 
    String[] keys
) {
    // 1. Check if user enabled
    if (!user.isOn) {
        return FALSE_WHEN_CHECK_CMD;
    }

    // 2. Check command permissions
    boolean isAllowed = false;
    boolean explicitlyDenied = false;

    for (RCmd rCmd : user.rCmdList) {
        if (rCmd.matches(command)) {
            isAllowed = true;
            break;
        }
    }

    for (RCmd rCmd : user.rCmdDisallowList) {
        if (rCmd.matches(command)) {
            explicitlyDenied = true;
            break;
        }
    }

    if (explicitDenied || !isAllowed) {
        return FALSE_WHEN_CHECK_CMD;
    }

    // 3. Check key permissions
    for (String key : keys) {
        boolean keyAllowed = false;

        for (RKey rKey : user.rKeyList) {
            if (rKey.matches(key)) {
                // Check read/write permissions
                if (rKey.canRead() || rKey.canWrite() || rKey.isReadWrite()) {
                    keyAllowed = true;
                    break;
                }
            }
        }

        if (!keyAllowed) {
            return FALSE_WHEN_CHECK_KEY;
        }
    }

    return TRUE;
}
```

## Key Permissions

### RKey Types

```java
public class RKey {
    enum Type {
        READ,           // Read-only access: %R~pattern*
        WRITE,          // Write-only access: %W~pattern*
        READ_WRITE,     // Access: %RW~pattern*
        ALL             // Access all keys: ~*
    }

    Type type;
    String pattern;   // Glob pattern (supports * and ?)
}
```

### Permission Patterns

```
Examples:
  ~*                     - All keys (read and write)
  %R~user:*              - User keys (read-only)
  %W~cache:*             - Cache keys (write-only)
  %RW~data:*             - Data keys (read and write)

  user:12345:profile     - Exact key (read and write)
  user:12345:session:*   - Wildcard pattern

  ~allkeys               - Same as ~*
```

### Glob Matching

```java
boolean matches(String key) {
    if ("%R~".equals(pattern.substring(0, 3))) {
        // Read-only: no write access
    }
    if ("%W~".equals(pattern.substring(0, 3))) {
        // Write-only: no read access  
    }

    // Extract pattern after ~
    String globPattern = pattern.replaceFirst("^.*~", "");

    // Use glob matching (supports * and ?)
    return matchesGlob(key, globPattern);
}
```

## PubSub Permissions

### RPubSub Type

```java
public class RPubSub {
    String pattern;   // Channel pattern: &channel*
}
```

### Channel Patterns

```
Examples:
  &*                      - All channels
  &user:*                  - Channels starting with "user:"
  &pub:*                   - Public channels
  allchannels              - Same as &*
```

## Authentication Flow

```
Client Connection
    │
    ├─> Send command
    │
    ├─> Is authenticated?
    │   └─> No → Require AUTH
    │
    ├─> AUTH username password
    │   ├─> User exists?
    │   │   └─> No → Return error
    │   ├─> Password matches?
    │   │   └─> No → Return error
    │   └─> OK → Set authenticated user
    │
    └─> Proceed with command
        ├─> Check user permissions
        ├─> Check command allow/deny lists
        └─> Check key permissions (if any)
```

## Error Replies

| Error Code | Reply | Meaning |
|-----------|-------|---------|
| AUTH_FAILED | NOAUTH | Wrong password |
| NO_AUTH | NOAUTH | Authentication required |
| ACL_PERMIT_LIMIT | NOPERM | Command permission denied |
| ACL_PERMIT_KEY_LIMIT | NOPERM | Key permission denied |
| SETUSER_RULE_INVALID | ERR | Invalid ACL rule syntax |

## Default User

```
user default on nopass +@all ~* &*

- Enabled by default
- No password required
- Access to all commands
- Access to all keys and channels
```

Note: For production, disable or set a password.

## Thread Safety

```java
@ThreadNeedLocal("net")
private static AclUsers[] perWorkerAclUsers;

// Each network worker has its own AclUsers instance
// Updates broadcast to all workers via socket messaging
```

## ACL Commands

```
ACL CAT [category]              - List commands in category
ACL DELUSER username           - Delete user
ACL DRYRUN username command ...  - Check permission
ACL GETUSER username           - Get user details
ACL LIST                       - List all users
ACL LOAD                       - Reload ACL from file
ACL LOG [count]                - Show ACL log
ACL SAVE                       - Save ACL to file
ACL SETUSER username [rules]   - Create/modify user
ACL USERS                      - List usernames
ACL WHOAMI                     - Show current user
```

## Related Documentation

- [Overall Architecture](./01_overall_architecture.md) - System overview
- [Command Processing Design](./04_command_processing_design.md) - Command permissions

## Key Source Files

- `src/main/java/io/velo/acl/AclUsers.java` - User management
- `src/main/java/io/velo/acl/U.java` - User model
- `src/main/java/io/velo/acl/RCmd.java` - Command permissions
- `src/main/java/io/velo/acl/RKey.java` - Key permissions
- `src/main/java/io/velo/acl/RPubSub.java` - Pub/Sub permissions
- `src/main/java/io/velo/acl/Category.java` - Command categories

---

**Version:** 1.0  
**Last Updated:** 2025-02-05  
**Author:** Velo Architecture Team
