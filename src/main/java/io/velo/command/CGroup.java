package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.net.socket.tcp.TcpSocket;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.ConfForGlobal;
import io.velo.SocketInspector;
import io.velo.VeloUserDataInSocket;
import io.velo.dyn.CachedGroovyClassLoader;
import io.velo.dyn.RefreshLoader;
import io.velo.reply.*;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Handles Redis commands starting with letter 'C'.
 * This includes commands like CLIENT, CLUSTER, COMMAND, CONFIG, COPY, etc.
 */
public class CGroup extends BaseCommand {
    /**
     * @param cmd    the command string
     * @param data   the data array
     * @param socket the TCP socket
     */
    public CGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    /**
     * Parses slot information from the command.
     *
     * @param cmd        the command name
     * @param data       the command arguments
     * @param slotNumber current slot number
     * @return list containing slot with key hash, or empty list
     */
    @Override
    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("copy".equals(cmd)) {
            if (data.length < 3) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            slotWithKeyHashList.add(slot(data[2], slotNumber));
            return slotWithKeyHashList;
        }

        // client can use any slot
        return slotWithKeyHashList;
    }

    /**
     * Handles the command and returns the reply.
     *
     * @return the reply for this command
     */
    @Override
    public Reply handle() {
        if ("client".equals(cmd)) {
            return client();
        }

        if ("cluster".equals(cmd) || "clusterx".equals(cmd)) {
            return clusterx();
        }

        if ("command".equals(cmd)) {
            return command();
        }

        if ("config".equals(cmd)) {
            return config();
        }

        if ("copy".equals(cmd)) {
            return copy();
        }

        return NilReply.INSTANCE;
    }

    /**
     * Sub-commands of {@code CLIENT} that Velo cannot yet support; reported as {@link ErrorReply#NOT_SUPPORT}
     * so clients that issue them (e.g. client-side caching, tracking, pause) get a clear error
     * instead of a silent nil or accidental no-op.
     */
    private static final ErrorReply CLIENT_SUBCOMMAND_UNSUPPORTED = ErrorReply.NOT_SUPPORT;

    @VisibleForTesting
    Reply client() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var subCmd = new String(data[1]).toLowerCase();
        if ("getname".equals(subCmd)) {
            if (data.length != 2) {
                return ErrorReply.FORMAT;
            }

            var veloUserData = SocketInspector.createUserDataIfNotSet(socket);
            var clientName = veloUserData.getClientName();
            return clientName == null ? NilReply.INSTANCE : new BulkReply(clientName);
        }

        if ("id".equals(subCmd)) {
            if (data.length != 2) {
                return ErrorReply.FORMAT;
            }
            // Prefer the monotonic clientId stamped at onConnect so CLIENT KILL ID <n> targets
            // the same value we report here. Fall back to hashCode() for callers that synthesized
            // a user data object without going through onConnect.
            var veloUserData = SocketInspector.createUserDataIfNotSet(socket);
            var clientId = veloUserData.getClientId();
            return new IntegerReply(clientId != 0L ? clientId : socket.hashCode());
        }

        if ("info".equals(subCmd)) {
            if (data.length != 2) {
                return ErrorReply.FORMAT;
            }
            var clientInfo = SocketInspector.getClientInfo(socket);
            return new BulkReply(clientInfo);
        }

        if ("list".equals(subCmd)) {
            return clientList();
        }

        if ("reply".equals(subCmd)) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            var replyModeStr = new String(data[2]).toLowerCase();
            var replyMode = VeloUserDataInSocket.ReplyMode.from(replyModeStr);

            var remoteAddress = ((TcpSocket) socket).getRemoteAddress();
            log.warn("Client {} change reply mode={}", remoteAddress, replyMode);

            var veloUserData = SocketInspector.createUserDataIfNotSet(socket);
            veloUserData.setReplyMode(replyMode);
            return replyMode == VeloUserDataInSocket.ReplyMode.on ? OKReply.INSTANCE : EmptyReply.INSTANCE;
        }

        if ("setinfo".equals(subCmd)) {
            if (data.length != 4 && data.length != 6) {
                return ErrorReply.FORMAT;
            }

            var veloUserData = SocketInspector.createUserDataIfNotSet(socket);

            final String optionLibName = "lib-name";
            final String optionLibVer = "lib-ver";
            for (int i = 2; i < data.length; i += 2) {
                var option = new String(data[i]).toLowerCase();
                if (optionLibName.equals(option)) {
                    veloUserData.setLibName(new String(data[i + 1]));
                } else if (optionLibVer.equals(option)) {
                    veloUserData.setLibVer(new String(data[i + 1]));
                } else {
                    return ErrorReply.SYNTAX;
                }
            }

            return OKReply.INSTANCE;
        }

        if ("setname".equals(subCmd)) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            var clientName = new String(data[2]);
            var veloUserData = SocketInspector.createUserDataIfNotSet(socket);
            veloUserData.setClientName(clientName);
            return OKReply.INSTANCE;
        }

        if ("kill".equals(subCmd)) {
            return clientKill();
        }

        // ----- safe no-op compatibility flags (Task 5) -----
        // Velo does not implement client-side eviction or LRU/LFU nudging, so the
        // CLIENT NO-EVICT and CLIENT NO-TOUCH flags are accepted as compatibility
        // no-ops. The on/off value is parsed for arity and recognized vocabulary.
        if ("no-evict".equals(subCmd) || "no-touch".equals(subCmd)) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }
            var value = new String(data[2]).toLowerCase();
            if (!"on".equals(value) && !"off".equals(value)) {
                return ErrorReply.SYNTAX;
            }
            return OKReply.INSTANCE;
        }

        // ----- explicitly unsupported stateful subcommands (Task 6) -----
        // These drive server-side behavior Velo has not implemented: invalidation
        // tracking, redirect slot for tracking, request admission pausing, blocked-
        // client unblocking. We surface NOT_SUPPORT so clients do not silently no-op.
        if ("caching".equals(subCmd)
                || "getredir".equals(subCmd)
                || "tracking".equals(subCmd)
                || "trackinginfo".equals(subCmd)
                || "pause".equals(subCmd)
                || "unpause".equals(subCmd)
                || "unblock".equals(subCmd)) {
            return CLIENT_SUBCOMMAND_UNSUPPORTED;
        }

        // Unknown CLIENT subcommand — Redis returns ERR syntax error. Returning SYNTAX
        // here (instead of the historical NilReply.INSTANCE) catches typos early.
        return ErrorReply.SYNTAX;
    }

    /**
     * CLIENT LIST — emits one {@link SocketInspector#getClientInfo} line per connected
     * socket. No {@code TYPE}/{@code ID} filters are recognized yet; any extra argument
     * is rejected as a format error to surface a likely bug in the caller.
     */
    @VisibleForTesting
    Reply clientList() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var socketInspector = localPersist.getSocketInspector();
        if (socketInspector == null) {
            return new BulkReply(new byte[0]);
        }

        var sb = new StringBuilder();
        var snapshot = new ArrayList<>(socketInspector.socketMap.values());
        for (var s : snapshot) {
            if (s != null) {
                sb.append(SocketInspector.getClientInfo(s));
            }
        }
        return new BulkReply(sb.toString().getBytes());
    }

    /**
     * CLIENT KILL — Redis-compatible filter-form KILL plus the legacy {@code ip:port}
     * form. Supports {@code ID}, {@code TYPE} (only {@code normal} and {@code pubsub}
     * actually match; {@code master} is accepted for Redis-compat but never matches
     * because Velo has no incoming master connection concept; {@code slave} and
     * {@code replica} are rejected at parse time with SYNTAX because real replication
     * sockets never reach {@link SocketInspector#socketMap}), {@code USER},
     * {@code ADDR}, {@code LADDR} (best-effort against
     * {@link ConfForGlobal#netListenAddresses}), {@code SKIPME} (default yes;
     * {@code no} lets the issuing socket be killed), and {@code MAXAGE}.
     */
    @VisibleForTesting
    Reply clientKill() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var filter = parseClientKillFilter();
        if (filter == null) {
            return ErrorReply.SYNTAX;
        }
        return killClientsMatching(filter);
    }

    /**
     * Parsed view of a {@code CLIENT KILL} filter form. {@code skipMe} defaults to true
     * to match Redis. All other fields are optional; only the set ones are matched.
     */
    private record ClientKillFilter(
            Long id,
            String type,
            String user,
            String addr,
            String laddr,
            boolean skipMe,
            Long maxAgeSeconds
    ) {
    }

    /**
     * Parses the filter form of {@code CLIENT KILL}. Returns {@code null} on any
     * unknown filter, malformed value, or odd arg count.
     */
    private ClientKillFilter parseClientKillFilter() {
        // Legacy form: CLIENT KILL ip:port (exactly three args, second is the addr).
        if (data.length == 3) {
            var legacyAddr = new String(data[2]);
            if (legacyAddr.contains(":")) {
                return new ClientKillFilter(null, null, null, legacyAddr, null, true, null);
            }
            // data.length == 3 with no colon in arg[2] is malformed — fall through to SYNTAX.
            return null;
        }

        Long id = null;
        String type = null;
        String user = null;
        String addr = null;
        String laddr = null;
        boolean skipMe = true;
        Long maxAgeSeconds = null;

        for (int i = 2; i < data.length; i += 2) {
            if (i + 1 >= data.length) {
                return null;
            }
            var name = new String(data[i]).toLowerCase();
            var value = new String(data[i + 1]);
            switch (name) {
                case "id" -> {
                    try {
                        id = Long.parseLong(value);
                    } catch (NumberFormatException e) {
                        return null;
                    }
                }
                case "type" -> {
                    var lower = value.toLowerCase();
                    if (!isSupportedClientKillType(lower)) {
                        return null;
                    }
                    type = lower;
                }
                case "user" -> user = value;
                case "addr" -> addr = value;
                case "laddr" -> laddr = value;
                case "skipme" -> {
                    if ("yes".equalsIgnoreCase(value)) {
                        skipMe = true;
                    } else if ("no".equalsIgnoreCase(value)) {
                        skipMe = false;
                    } else {
                        return null;
                    }
                }
                case "maxage" -> {
                    try {
                        maxAgeSeconds = Long.parseLong(value);
                        if (maxAgeSeconds < 0) {
                            return null;
                        }
                    } catch (NumberFormatException e) {
                        return null;
                    }
                }
                default -> {
                    return null;
                }
            }
        }

        return new ClientKillFilter(id, type, user, addr, laddr, skipMe, maxAgeSeconds);
    }

    /**
     * Recognized {@code TYPE} filter values. Only {@code normal} and {@code pubsub}
     * are honored at match time. {@code master} is accepted in parsing (Redis
     * compatibility) but never matches because Velo has no incoming master
     * connection role. {@code slave} and {@code replica} are rejected at parse time
     * with SYNTAX because real replication sockets are not tracked in
     * {@link SocketInspector#socketMap} (their {@code onConnect} returns early), so
     * Velo cannot iterate them in {@code CLIENT KILL}.
     */
    private static boolean isSupportedClientKillType(String type) {
        return "normal".equals(type)
                || "pubsub".equals(type)
                || "master".equals(type);
    }

    /**
     * Returns the logical Redis client type for {@code candidate} based on what Velo
     * tracks: {@code pubsub} for sockets that appear in any subscription map,
     * {@code normal} otherwise. The {@code replica} return is retained defensively
     * (real repl sockets are short-circuited in {@link SocketInspector#onConnect} and
     * never reach {@code socketMap}); the {@code slave}/{@code replica} parse-time
     * rejection in {@link #isSupportedClientKillType(String)} makes this branch
     * unreachable for {@code CLIENT KILL} but keeps the method honest about what it
     * could observe.
     */
    private String clientType(ITcpSocket candidate) {
        var ud = (VeloUserDataInSocket) ((TcpSocket) candidate).getUserData();
        if (ud != null && ud.getReplPairAsSlaveInTcpClient() != null) {
            return "replica";
        }
        var inspector = localPersist.getSocketInspector();
        if (inspector != null && inspector.isSubscribed(candidate)) {
            return "pubsub";
        }
        return "normal";
    }

    /**
     * Decides whether a candidate socket matches a parsed filter.
     */
    private boolean matchesClientKillFilter(ITcpSocket candidate, ClientKillFilter filter) {
        if (filter.skipMe() && candidate == socket) {
            return false;
        }
        if (filter.id() != null) {
            var ud = (VeloUserDataInSocket) ((TcpSocket) candidate).getUserData();
            if (ud == null || ud.getClientId() != filter.id()) {
                return false;
            }
        }
        if (filter.type() != null) {
            // Only normal / pubsub / master are accepted in parsing (see
            // isSupportedClientKillType). master never matches because Velo has
            // no incoming master connection concept, so a TYPE master filter
            // always returns 0 killed — which is the documented Redis-compatible
            // behavior on a non-replica server.
            var t = clientType(candidate);
            if (!filter.type().equals(t)) {
                return false;
            }
        }
        if (filter.user() != null) {
            var authUser = SocketInspector.getAuthUser(candidate);
            var user = authUser == null ? "default" : authUser;
            if (!filter.user().equals(user)) {
                return false;
            }
        }
        if (filter.addr() != null) {
            // Normalize through formatRedisAddress so the comparison is in the
            // same ip:port form CLIENT LIST emits, not InetSocketAddress.toString()
            // (which prepends "host/" for resolved hostnames).
            var remoteAddress = SocketInspector.formatRedisAddress(((TcpSocket) candidate).getRemoteAddress());
            if (!filter.addr().equals(remoteAddress)) {
                return false;
            }
        }
        if (filter.laddr() != null) {
            // LADDR is best-effort: Velo reports the configured listen addresses, not the
            // concrete local address of the accepted socket. Match if any configured
            // listen address equals the filter value.
            if (!filter.laddr().equals(ConfForGlobal.netListenAddresses)) {
                return false;
            }
        }
        if (filter.maxAgeSeconds() != null) {
            var ud = SocketInspector.createUserDataIfNotSet(candidate);
            var ageSeconds = (System.currentTimeMillis() - ud.getConnectedTimeMillis()) / 1000L;
            if (ageSeconds < filter.maxAgeSeconds()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Iterates the current socket map and closes every match on its owning reactor
     * thread (mirroring {@link MultiWorkerServer#handleQuit} so a slot worker can
     * close a socket owned by a different net-worker reactor).
     */
    private Reply killClientsMatching(ClientKillFilter filter) {
        var socketInspector = localPersist.getSocketInspector();
        if (socketInspector == null) {
            return new IntegerReply(0L);
        }

        int killed = 0;
        var snapshot = new ArrayList<>(socketInspector.socketMap.entrySet());
        for (var entry : snapshot) {
            var addr = entry.getKey();
            var s = entry.getValue();
            if (s == null) {
                continue;
            }
            if (!matchesClientKillFilter(s, filter)) {
                continue;
            }
            try {
                s.getReactor().submit(() -> {
                    try {
                        s.close();
                    } catch (Exception ex) {
                        log.warn("Client kill close error, addr={}, msg={}", addr, ex.getMessage());
                    }
                });
                killed++;
            } catch (Exception e) {
                log.warn("Client kill error, addr={}, msg={}", addr, e.getMessage());
            }
        }

        return new IntegerReply(killed);
    }

    private final CachedGroovyClassLoader cl = CachedGroovyClassLoader.getInstance();

    @VisibleForTesting
    Reply clusterx() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var scriptText = RefreshLoader.getScriptText("/dyn/src/io/velo/script/ClusterxCommandHandle.groovy");

        var variables = new HashMap<String, Object>();
        variables.put("cGroup", this);
        return (Reply) cl.eval(scriptText, variables);
    }

    @VisibleForTesting
    Reply command() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var scriptText = RefreshLoader.getScriptText("/dyn/src/io/velo/script/CommandCommandHandle.groovy");

        var variables = new HashMap<String, Object>();
        variables.put("cGroup", this);
        return (Reply) cl.eval(scriptText, variables);
    }

    @VisibleForTesting
    Reply config() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var scriptText = RefreshLoader.getScriptText("/dyn/src/io/velo/script/ConfigCommandHandle.groovy");

        var variables = new HashMap<String, Object>();
        variables.put("cGroup", this);
        return (Reply) cl.eval(scriptText, variables);
    }

    @VisibleForTesting
    Reply copy() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var srcKeyBytes = data[1];
        var dstKeyBytes = data[2];

        boolean replace = false;
        for (int i = 3; i < data.length; i++) {
            if ("replace".equalsIgnoreCase(new String(data[i]))) {
                replace = true;
                break;
            }
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var srcCv = getCv(slotWithKeyHash);
        if (srcCv == null) {
            return IntegerReply.REPLY_0;
        }

        var dstSlotWithKeyHash = slotWithKeyHashListParsed.getLast();
        if (isCrossRequestWorker) {
            // current slot worker is src key slot's slot worker
            var dstSlot = dstSlotWithKeyHash.slot();
            var dstOneSlot = localPersist.oneSlot(dstSlot);

            SettablePromise<Reply> finalPromise = new SettablePromise<>();
            var asyncReply = new AsyncReply(finalPromise);

            boolean finalReplace = replace;
            dstOneSlot.asyncExecute(() -> {
                var dstCv = getCv(dstSlotWithKeyHash);
                if (dstCv != null && !finalReplace) {
                    finalPromise.set(IntegerReply.REPLY_0);
                    return;
                }

                setCv(srcCv, dstSlotWithKeyHash);
                finalPromise.set(IntegerReply.REPLY_1);
            });

            return asyncReply;
        } else {
            var existCv = getCv(dstSlotWithKeyHash);
            if (existCv != null && !replace) {
                return IntegerReply.REPLY_0;
            }

            setCv(srcCv, dstSlotWithKeyHash);
            return IntegerReply.REPLY_1;
        }
    }
}
