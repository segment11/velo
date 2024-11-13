package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.Dict;
import io.velo.TrainSampleJob;
import io.velo.reply.*;
import io.velo.type.RedisZSet;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Arrays;

public class ZGroup extends BaseCommand {
    public ZGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("zadd".equals(cmd) || "zcard".equals(cmd) || "zcount".equals(cmd)
                || "zincrby".equals(cmd)
                || "zlexcount".equals(cmd) || "zmscore".equals(cmd)
                || "zpopmax".equals(cmd) || "zpopmin".equals(cmd)
                || "zrandmember".equals(cmd)
                || "zrange".equals(cmd) || "zrangebylex".equals(cmd) || "zrangebyscore".equals(cmd)
                || "zrank".equals(cmd)
                || "zrem".equals(cmd) || "zremrangebylex".equals(cmd) || "zremrangebyrank".equals(cmd) || "zremrangebyscore".equals(cmd)
                || "zrevrange".equals(cmd) || "zrevrangebylex".equals(cmd) || "zrevrangebyscore".equals(cmd) || "zrevrank".equals(cmd)
                || "zscore".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            var keyBytes = data[1];
            var slotWithKeyHash = slot(keyBytes, slotNumber);
            slotWithKeyHashList.add(slotWithKeyHash);
            return slotWithKeyHashList;
        }

        if ("zdiff".equals(cmd) || "zinter".equals(cmd) || "zintercard".equals(cmd) || "zunion".equals(cmd)) {
            if (data.length < 4) {
                return slotWithKeyHashList;
            }

            var numKeysBytes = data[1];
            int numKeys;
            try {
                numKeys = Integer.parseInt(new String(numKeysBytes));
            } catch (NumberFormatException e) {
                throw new ErrorReplyException(ErrorReply.NOT_INTEGER.getMessage());
            }

            if (numKeys < 2) {
                throw new ErrorReplyException(ErrorReply.INVALID_INTEGER.getMessage());
            }

            if (data.length < numKeys + 2) {
                throw new ErrorReplyException(ErrorReply.SYNTAX.getMessage());
            }

            for (int i = 2; i < numKeys + 2; i++) {
                var keyBytes = data[i];
                var slotWithKeyHash = slot(keyBytes, slotNumber);
                slotWithKeyHashList.add(slotWithKeyHash);
            }
            return slotWithKeyHashList;
        }

        if ("zdiffstore".equals(cmd) || "zinterstore".equals(cmd) || "zunionstore".equals(cmd)) {
            if (data.length < 5) {
                return slotWithKeyHashList;
            }

            // reuse zdiff method, need to keep the order, exclude dst key
//            var dstKeyBytes = data[1];
//            var dstSlotWithKeyHash = slot(dstKeyBytes, slotNumber);
//            slotWithKeyHashList.add(dstSlotWithKeyHash);

            var numKeysBytes = data[2];
            int numKeys;
            try {
                numKeys = Integer.parseInt(new String(numKeysBytes));
            } catch (NumberFormatException e) {
                throw new ErrorReplyException(ErrorReply.NOT_INTEGER.getMessage());
            }

            if (numKeys < 2) {
                throw new ErrorReplyException(ErrorReply.INVALID_INTEGER.getMessage());
            }

            if (data.length < numKeys + 3) {
                throw new ErrorReplyException(ErrorReply.SYNTAX.getMessage());
            }

            for (int i = 3; i < numKeys + 3; i++) {
                var keyBytes = data[i];
                var slotWithKeyHash = slot(keyBytes, slotNumber);
                slotWithKeyHashList.add(slotWithKeyHash);
            }
            return slotWithKeyHashList;
        }

        if ("zrangestore".equals(cmd)) {
            if (data.length < 5) {
                return slotWithKeyHashList;
            }

            // dst first, src last
            var dstKeyBytes = data[1];
            var srcKeyBytes = data[2];

            var s1 = slot(srcKeyBytes, slotNumber);
            var s2 = slot(dstKeyBytes, slotNumber);
            // add s1 first, important!!!
            // so can reuse zrange method
            slotWithKeyHashList.add(s1);
            slotWithKeyHashList.add(s2);
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
        // sorted set group
        // like set group in SGroup
        if ("zadd".equals(cmd)) {
            return zadd();
        }

        if ("zcard".equals(cmd)) {
            return zcard();
        }

        if ("zcount".equals(cmd)) {
            return zcount(false);
        }

        if ("zdiff".equals(cmd)) {
            return zdiff(data, false, false, null);
        }

        if ("zdiffstore".equals(cmd)) {
            return zdiffstore(false, false);
        }

        if ("zincrby".equals(cmd)) {
            return zincrby();
        }

        if ("zinter".equals(cmd)) {
            return zdiff(data, true, false, null);
        }

        if ("zintercard".equals(cmd)) {
            return zintercard();
        }

        if ("zinterstore".equals(cmd)) {
            return zdiffstore(true, false);
        }

        if ("zlexcount".equals(cmd)) {
            return zcount(true);
        }

        if ("zmscore".equals(cmd)) {
            return zmscore();
        }

        if ("zpopmax".equals(cmd)) {
            return zpopmax(false);
        }

        if ("zpopmin".equals(cmd)) {
            return zpopmax(true);
        }

        if ("zrandmember".equals(cmd)) {
            return zrandmember();
        }

        if ("zrange".equals(cmd)) {
            return zrange(data);
        }

        if ("zrangebylex".equals(cmd)) {
            if (data.length != 4 && data.length != 7) {
                return ErrorReply.FORMAT;
            }

            var dd = new byte[data.length + 1][];
            dd[0] = data[0];
            dd[1] = data[1];
            dd[2] = data[2];
            dd[3] = data[3];

            dd[4] = "bylex".getBytes();
            System.arraycopy(data, 4, dd, 5, data.length - 4);

            return zrange(dd);
        }

        if ("zrangebyscore".equals(cmd)) {
            if (data.length < 4) {
                return ErrorReply.FORMAT;
            }

            var dd = new byte[data.length + 1][];
            dd[0] = data[0];
            dd[1] = data[1];
            dd[2] = data[2];
            dd[3] = data[3];

            dd[4] = "byscore".getBytes();
            System.arraycopy(data, 4, dd, 5, data.length - 4);

            return zrange(dd);
        }

        if ("zrangestore".equals(cmd)) {
            if (data.length < 5) {
                return ErrorReply.FORMAT;
            }

            var dd = new byte[data.length - 1][];
            dd[0] = data[0];
            dd[1] = data[2];
            dd[2] = data[3];
            dd[3] = data[4];
            System.arraycopy(data, 5, dd, 4, data.length - 5);

            var dstKeyBytes = data[1];
            return zrange(dd, dstKeyBytes);
        }

        if ("zrank".equals(cmd)) {
            return zrank(false);
        }

        if ("zrem".equals(cmd)) {
            return zrem();
        }

        if ("zremrangebylex".equals(cmd)) {
            return zremrangebyscore(false, true, false);
        }

        if ("zremrangebyrank".equals(cmd)) {
            return zremrangebyscore(false, false, true);
        }

        if ("zremrangebyscore".equals(cmd)) {
            return zremrangebyscore(true, false, false);
        }

        if ("zrevrange".equals(cmd)) {
            if (data.length < 4) {
                return ErrorReply.FORMAT;
            }

            var dd = new byte[data.length + 1][];
            dd[0] = data[0];
            dd[1] = data[1];
            dd[2] = data[2];
            dd[3] = data[3];

            dd[4] = "rev".getBytes();
            System.arraycopy(data, 4, dd, 5, data.length - 4);

            return zrange(dd);
        }

        if ("zrevrangebylex".equals(cmd)) {
            if (data.length < 4) {
                return ErrorReply.FORMAT;
            }

            var dd = new byte[data.length + 2][];
            dd[0] = data[0];
            dd[1] = data[1];
            dd[2] = data[2];
            dd[3] = data[3];

            dd[4] = "rev".getBytes();
            dd[5] = "bylex".getBytes();
            System.arraycopy(data, 4, dd, 6, data.length - 4);

            return zrange(dd);
        }

        if ("zrevrangebyscore".equals(cmd)) {
            if (data.length < 4) {
                return ErrorReply.FORMAT;
            }

            var dd = new byte[data.length + 2][];
            dd[0] = data[0];
            dd[1] = data[1];
            dd[2] = data[2];
            dd[3] = data[3];

            dd[4] = "rev".getBytes();
            dd[5] = "byscore".getBytes();
            System.arraycopy(data, 4, dd, 6, data.length - 4);

            return zrange(dd);
        }

        if ("zrevrank".equals(cmd)) {
            return zrank(true);
        }

        if ("zscore".equals(cmd)) {
            return zscore();
        }

        if ("zunion".equals(cmd)) {
            return zdiff(data, false, true, null);
        }

        if ("zunionstore".equals(cmd)) {
            return zdiffstore(false, true);
        }

        return NilReply.INSTANCE;
    }

    private RedisZSet getRedisZSet(byte[] keyBytes, SlotWithKeyHash slotWithKeyHash) {
        var encodedBytes = get(keyBytes, slotWithKeyHash, false, CompressedValue.SP_TYPE_ZSET);
        if (encodedBytes == null) {
            return null;
        }

        return RedisZSet.decode(encodedBytes);
    }

    private void saveRedisZSet(RedisZSet rz, byte[] keyBytes, SlotWithKeyHash slotWithKeyHash) {
        var key = new String(keyBytes);
        if (rz.isEmpty()) {
            removeDelay(slotWithKeyHash.slot(), slotWithKeyHash.bucketIndex(), key, slotWithKeyHash.keyHash());
            return;
        }

        var keyPrefixOrSuffix = TrainSampleJob.keyPrefixOrSuffixGroup(key);
        var preferDict = dictMap.getDict(keyPrefixOrSuffix);
        if (preferDict == null) {
            preferDict = Dict.SELF_ZSTD_DICT;
        }
        set(keyBytes, rz.encode(preferDict), slotWithKeyHash, CompressedValue.SP_TYPE_ZSET);
    }

    private record Member(double score, String e) {
    }

    @VisibleForTesting
    Reply zadd() {
        if (data.length < 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var scoreBeginIndex = 2;

        boolean isNx = false;
        boolean isXx = false;
        boolean isGt = false;
        boolean isLt = false;
        boolean isIncludeCh = false;
        boolean isIncr = false;

        for (int i = 2; i < data.length; i++) {
            var tmpBytes = data[i];
            var tmp = new String(tmpBytes).toLowerCase();
            if ("nx".equals(tmp)) {
                isNx = true;
                scoreBeginIndex++;
            } else if ("xx".equals(tmp)) {
                isXx = true;
                scoreBeginIndex++;
            } else if ("gt".equals(tmp)) {
                isGt = true;
                scoreBeginIndex++;
            } else if ("lt".equals(tmp)) {
                isLt = true;
                scoreBeginIndex++;
            } else if ("ch".equals(tmp)) {
                isIncludeCh = true;
                scoreBeginIndex++;
            } else if ("incr".equals(tmp)) {
                isIncr = true;
                scoreBeginIndex++;
            } else {
                break;
            }
        }

        if (isIncr) {
            // only support one pair
            if (data.length != scoreBeginIndex + 2) {
                return ErrorReply.SYNTAX;
            }
        }

        var leftLength = data.length - scoreBeginIndex;
        if (leftLength <= 0 || leftLength % 2 != 0) {
            return ErrorReply.SYNTAX;
        }

        // multi e
        ArrayList<Member> members = new ArrayList<>();
        for (int i = scoreBeginIndex; i < data.length; i += 2) {
            var scoreBytes = data[i];
            double score;
            try {
                score = Double.parseDouble(new String(scoreBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_FLOAT;
            }

            var memberBytes = data[i + 1];
            if (memberBytes.length > RedisZSet.ZSET_MEMBER_MAX_LENGTH) {
                return ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG;
            }

            members.add(new Member(score, new String(memberBytes)));
        }

        // use RedisZSet to store zset
        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rz = getRedisZSet(keyBytes, slotWithKeyHash);
        if (rz == null) {
            rz = new RedisZSet();
        }

        int added = 0;
        int changed = 0;
        for (var member : members) {
            if (isNx) {
                // nx
                if (rz.contains(member.e)) {
                    continue;
                }
            } else if (isXx) {
                // xx
                if (!rz.contains(member.e)) {
                    continue;
                }
            }

            var svExist = rz.get(member.e);
            boolean isNew = svExist == null;
            if (!isNew) {
                if (isGt) {
                    // gt
                    if (svExist.score() <= member.score) {
                        continue;
                    }
                } else if (isLt) {
                    // lt
                    if (svExist.score() >= member.score) {
                        continue;
                    }
                }
            }

            var newScore = member.score;
            if (isIncr) {
                // incr
                if (!isNew) {
                    newScore += svExist.score();
                }
            }

            if (!isNew) {
                if (newScore == svExist.score()) {
                    continue;
                }
            }

            if (isNew && rz.size() >= RedisZSet.ZSET_MAX_SIZE) {
                return ErrorReply.ZSET_SIZE_TO_LONG;
            }

            rz.add(newScore, member.e);
            if (isNew) {
                added++;
            } else {
                changed++;
            }
        }

        var handled = added + changed;
        if (handled > 0) {
            saveRedisZSet(rz, keyBytes, slotWithKeyHash);
        }
        return new IntegerReply(isIncludeCh ? changed + added : added);
    }

    @VisibleForTesting
    Reply zcard() {
        if (data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var encodedBytes = get(keyBytes, slotWithKeyHash, false, CompressedValue.SP_TYPE_ZSET);
        if (encodedBytes == null) {
            return IntegerReply.REPLY_0;
        }

        var size = RedisZSet.getSizeWithoutDecode(encodedBytes);
        return new IntegerReply(size);
    }

    private final static String inf = "+inf";

    private final static String negInf = "-inf";

    @VisibleForTesting
    Reply zcount(boolean byLex) {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var minBytes = data[2];
        var maxBytes = data[3];
        var minStr = new String(minBytes).toLowerCase();
        var maxStr = new String(maxBytes).toLowerCase();

        String minLex = null;
        String maxLex = null;
        boolean minInclusive = true;
        boolean maxInclusive = true;

        if (byLex) {
            if (minBytes[0] != '(' && minBytes[0] != '[') {
                return ErrorReply.SYNTAX;
            }
            if (maxBytes[0] != '(' && maxBytes[0] != '[') {
                return ErrorReply.SYNTAX;
            }

            minLex = minStr.substring(1);
            maxLex = maxStr.substring(1);

            if (minLex.compareTo(maxLex) > 0) {
                return IntegerReply.REPLY_0;
            }

            if (minBytes[0] == '(') {
                minInclusive = false;
            }
            if (maxBytes[0] == '(') {
                maxInclusive = false;
            }
        }

        double min = 0;
        double max = 0;
        if (!byLex) {
            if (negInf.equals(minStr)) {
                min = Double.NEGATIVE_INFINITY;
            } else {
                int beginIndex = 0;
                if (minBytes[0] == '(') {
                    minInclusive = false;
                    beginIndex = 1;
                } else if (minBytes[0] == '[') {
                    beginIndex = 1;
                }
                try {
                    min = Double.parseDouble(minStr.substring(beginIndex));
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }
            }

            if (inf.equals(maxStr)) {
                max = Double.POSITIVE_INFINITY;
            } else {
                int beginIndex = 0;
                if (maxBytes[0] == '(') {
                    maxInclusive = false;
                    beginIndex = 1;
                } else if (maxBytes[0] == '[') {
                    beginIndex = 1;
                }
                try {
                    max = Double.parseDouble(maxStr.substring(beginIndex));
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }
            }

            if (min > max) {
                return IntegerReply.REPLY_0;
            }
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rz = getRedisZSet(keyBytes, slotWithKeyHash);
        if (rz == null || rz.isEmpty()) {
            return IntegerReply.REPLY_0;
        }

        if (!byLex) {
            var subSet = rz.between(min, minInclusive, max, maxInclusive);
            return new IntegerReply(subSet.size());
        } else {
            var n = rz.betweenByMember(minLex, minInclusive, maxLex, maxInclusive).size();
            return new IntegerReply(n);
        }
    }

    private void operateZset(RedisZSet rz, ArrayList<RedisZSet> otherRzList, boolean isInter, boolean isUnion,
                             boolean isAggregateSum, boolean isAggregateMin, boolean isAggregateMax,
                             boolean isWeights, double[] weights) {
        if (isInter) {
            var memberMap = rz.getMemberMap();

            int otherKeyIndex = 0;
            outer:
            for (var otherRz : otherRzList) {
                if (otherRz == null || otherRz.isEmpty()) {
                    rz.clear();
                    break;
                }

                var it = rz.getSet().iterator();
                while (it.hasNext()) {
                    var sv = it.next();
                    var otherSv = otherRz.get(sv.member());
                    if (otherSv == null) {
                        it.remove();
                        memberMap.remove(sv.member());

                        if (memberMap.isEmpty()) {
                            break outer;
                        }

                        continue;
                    }

                    double memberScore = sv.score();
                    if (!sv.isAlreadyWeighted) {
                        if (isWeights) {
                            memberScore *= weights[0];
                        }
                        sv.score(memberScore);
                        sv.isAlreadyWeighted = true;
                    }

                    var otherMemberScore = isWeights ? weights[otherKeyIndex + 1] * otherSv.score() : otherSv.score();
                    if (isAggregateSum) {
                        sv.score(memberScore + otherMemberScore);
                    } else if (isAggregateMin) {
                        sv.score(Math.min(memberScore, otherMemberScore));
                    } else {
                        sv.score(Math.max(memberScore, otherMemberScore));
                    }
                }

                otherKeyIndex++;
            }
        } else if (isUnion) {
            int otherKeyIndex = 0;
            for (var otherRz : otherRzList) {
                if (otherRz == null || otherRz.isEmpty()) {
                    otherKeyIndex++;
                    continue;
                }

                for (var otherSv : otherRz.getSet()) {
                    var sv = rz.get(otherSv.member());
                    if (sv == null) {
                        double memberScore = otherSv.score();
                        if (isWeights) {
                            memberScore *= weights[otherKeyIndex + 1];
                        }

                        rz.add(memberScore, otherSv.member(), true, true);
                        if (rz.size() == RedisZSet.ZSET_MAX_SIZE) {
                            throw new ErrorReplyException(ErrorReply.ZSET_SIZE_TO_LONG.getMessage());
                        }
                    } else {
                        double memberScore = sv.score();
                        if (!sv.isAlreadyWeighted) {
                            if (isWeights) {
                                memberScore *= weights[0];
                            }
                            sv.score(memberScore);
                            sv.isAlreadyWeighted = true;
                        }

                        var otherMemberScore = isWeights ? weights[otherKeyIndex + 1] * otherSv.score() : otherSv.score();
                        if (isAggregateSum) {
                            sv.score(memberScore + otherMemberScore);
                        } else if (isAggregateMin) {
                            sv.score(Math.min(memberScore, otherMemberScore));
                        } else {
                            sv.score(Math.max(memberScore, otherMemberScore));
                        }
                    }
                }

                otherKeyIndex++;
            }
        } else {
            // diff
            outer:
            for (var otherRz : otherRzList) {
                if (otherRz == null || otherRz.isEmpty()) {
                    continue;
                }
                for (var otherSv : otherRz.getSet()) {
                    if (rz.contains(otherSv.member())) {
                        rz.remove(otherSv.member());

                        if (rz.isEmpty()) {
                            break outer;
                        }
                    }
                }
            }
        }
    }

    @VisibleForTesting
    Reply zdiff(boolean isInter, boolean isUnion) {
        return zdiff(data, isInter, isUnion, null);
    }

    @TestOnly
    void addDstKeyBytesForStore(byte[] dstKeyBytes) {
        var data2 = new byte[data.length + 1][];
        data2[0] = data[0];
        data2[1] = dstKeyBytes;
        System.arraycopy(data, 1, data2, 2, data.length - 1);
        this.data = data2;
    }

    @VisibleForTesting
    Reply zdiffstore(boolean isInter, boolean isUnion) {
        if (data.length < 5) {
            return ErrorReply.FORMAT;
        }

        var dd = new byte[data.length - 1][];
        dd[0] = data[0];
        dd[1] = data[2];
        dd[2] = data[3];
        dd[3] = data[4];
        System.arraycopy(data, 5, dd, 4, data.length - 5);

        var dstKeyBytes = data[1];
        return zdiff(dd, isInter, isUnion, dstKeyBytes);
    }

    @VisibleForTesting
    Reply zdiff(byte[][] dd, boolean isInter, boolean isUnion, byte[] dstKeyBytes) {
        if (dd.length < 4) {
            return ErrorReply.FORMAT;
        }

        var doStore = dstKeyBytes != null;
        var dstSlotWithKeyHash = doStore ? slot(dstKeyBytes) : null;

        var numKeysBytes = dd[1];
        int numKeys;
        try {
            numKeys = Integer.parseInt(new String(numKeysBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        if (numKeys < 2) {
            return ErrorReply.INVALID_INTEGER;
        }

        if (dd.length < numKeys + 2) {
            return ErrorReply.SYNTAX;
        }

        // already checked when parse slots
//        int numKeys = Integer.parseInt(new String(dd[1]));

        boolean isAggregateSum = true;
        boolean isAggregateMin = false;
        boolean isAggregateMax = false;

        boolean isWeights = false;
        double[] weights = null;

        boolean withScores = false;

        if (isInter || isUnion) {
            for (int i = 2 + numKeys; i < dd.length; i++) {
                var tmpBytes = dd[i];
                var tmp = new String(tmpBytes).toLowerCase();

                if ("aggregate".equals(tmp)) {
                    if (i + 1 >= dd.length) {
                        return ErrorReply.SYNTAX;
                    }
                    var aggregateBytes = dd[i + 1];
                    var aggregate = new String(aggregateBytes).toLowerCase();
                    if ("sum".equals(aggregate)) {
                        isAggregateSum = true;
                        isAggregateMin = false;
                        isAggregateMax = false;
                    } else if ("min".equals(aggregate)) {
                        isAggregateSum = false;
                        isAggregateMin = true;
                        isAggregateMax = false;
                    } else if ("max".equals(aggregate)) {
                        isAggregateSum = false;
                        isAggregateMin = false;
                        isAggregateMax = true;
                    } else {
                        return ErrorReply.SYNTAX;
                    }
                    i++;
                } else if ("weights".equals(tmp)) {
                    isWeights = true;

                    if (i + numKeys >= dd.length) {
                        return ErrorReply.SYNTAX;
                    }

                    weights = new double[numKeys];
                    for (int j = 0; j < numKeys; j++) {
                        var weightBytes = dd[i + 1 + j];
                        double weight;
                        try {
                            weight = Double.parseDouble(new String(weightBytes));
                        } catch (NumberFormatException e) {
                            return ErrorReply.NOT_FLOAT;
                        }
                        weights[j] = weight;
                    }
                } else if ("withscores".equals(tmp)) {
                    withScores = true;
                }
            }
        } else {
            // withscores
            if (dd.length != numKeys + 2 && dd.length != numKeys + 3) {
                return ErrorReply.SYNTAX;
            }

            withScores = "withscores".equalsIgnoreCase(new String(dd[dd.length - 1]));
        }

        // begin from 2
        for (int i = 2, j = 0; i < numKeys + 2; i++, j++) {
            var keyBytes = dd[i];
            if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
        }

        var first = slotWithKeyHashListParsed.getFirst();
        var rz = getRedisZSet(first.rawKey().getBytes(), first);
        if (rz == null || rz.isEmpty()) {
            if (isInter) {
                return doStore ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
            }
            if (!isUnion) {
                return doStore ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
            }
            if (rz == null) {
                rz = new RedisZSet();
            }
        }

        if (!isCrossRequestWorker) {
            ArrayList<RedisZSet> otherRzList = new ArrayList<>(slotWithKeyHashListParsed.size() - 1);
            for (int i = 1; i < slotWithKeyHashListParsed.size(); i++) {
                var other = slotWithKeyHashListParsed.get(i);
                var otherRz = getRedisZSet(other.rawKey().getBytes(), other);
                otherRzList.add(otherRz);
            }
            operateZset(rz, otherRzList, isInter, isUnion,
                    isAggregateSum, isAggregateMin, isAggregateMax,
                    isWeights, weights);

            if (rz.isEmpty()) {
                return doStore ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
            }

            if (doStore) {
                saveRedisZSet(rz, dstKeyBytes, dstSlotWithKeyHash);
                return new IntegerReply(rz.size());
            } else {
                var replies = new Reply[rz.size() * (withScores ? 2 : 1)];
                int i = 0;
                for (var sv : rz.getSet()) {
                    replies[i++] = new BulkReply(sv.member().getBytes());
                    if (withScores) {
                        replies[i++] = new BulkReply(String.valueOf(sv.score()).getBytes());
                    }
                }
                return new MultiBulkReply(replies);
            }
        }

        ArrayList<Promise<RedisZSet>> promises = new ArrayList<>(slotWithKeyHashListParsed.size() - 1);
        for (int i = 1; i < slotWithKeyHashListParsed.size(); i++) {
            var other = slotWithKeyHashListParsed.get(i);

            var oneSlot = localPersist.oneSlot(other.slot());
            var p = oneSlot.asyncCall(() -> getRedisZSet(other.rawKey().getBytes(), other));
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        boolean finalIsAggregateSum = isAggregateSum;
        boolean finalIsAggregateMin = isAggregateMin;
        boolean finalIsAggregateMax = isAggregateMax;
        boolean finalIsWeights = isWeights;
        double[] finalWeights = weights;
        boolean finalWithScores = withScores;
        RedisZSet finalRz = rz;
        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("zdiff error={}, isInter={}, isUnion={}", e.getMessage(), isInter, isUnion);
                finalPromise.setException(e);
                return;
            }

            ArrayList<RedisZSet> otherRzList = new ArrayList<>(slotWithKeyHashListParsed.size() - 1);
            for (var promise : promises) {
                otherRzList.add(promise.getResult());
            }
            operateZset(finalRz, otherRzList, isInter, isUnion,
                    finalIsAggregateSum, finalIsAggregateMin, finalIsAggregateMax,
                    finalIsWeights, finalWeights);

            if (finalRz.isEmpty()) {
                finalPromise.set(doStore ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY);
                return;
            }

            if (doStore) {
                var dstOneSlot = localPersist.oneSlot(dstSlotWithKeyHash.slot());
                dstOneSlot.asyncRun(() -> saveRedisZSet(finalRz, dstKeyBytes, dstSlotWithKeyHash));
                finalPromise.set(new IntegerReply(finalRz.size()));
            } else {
                var replies = new Reply[finalRz.size() * (finalWithScores ? 2 : 1)];
                int i = 0;
                for (var sv : finalRz.getSet()) {
                    replies[i++] = new BulkReply(sv.member().getBytes());
                    if (finalWithScores) {
                        replies[i++] = new BulkReply(String.valueOf(sv.score()).getBytes());
                    }
                }
                finalPromise.set(new MultiBulkReply(replies));
            }
        });

        return asyncReply;
    }

    Reply zincrby() {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var scoreBytes = data[2];
        double score;
        try {
            score = Double.parseDouble(new String(scoreBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_FLOAT;
        }

        var memberBytes = data[3];
        if (memberBytes.length > RedisZSet.ZSET_MEMBER_MAX_LENGTH) {
            return ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rz = getRedisZSet(keyBytes, slotWithKeyHash);
        if (rz == null) {
            rz = new RedisZSet();
        }

        var member = new String(memberBytes);
        var svExist = rz.get(member);
        if (svExist != null) {
            score += svExist.score();
        } else {
            if (rz.size() >= RedisZSet.ZSET_MAX_SIZE) {
                return ErrorReply.ZSET_SIZE_TO_LONG;
            }
        }

        rz.add(score, member);

        saveRedisZSet(rz, keyBytes, slotWithKeyHash);
        return new BulkReply(String.valueOf(score).getBytes());
    }

    @VisibleForTesting
    Reply zintercard() {
        if (data.length < 4) {
            return ErrorReply.FORMAT;
        }

        var numKeysBytes = data[1];
        int numKeys;
        try {
            numKeys = Integer.parseInt(new String(numKeysBytes));
        } catch (NumberFormatException e) {
            return ErrorReply.NOT_INTEGER;
        }

        if (numKeys < 2) {
            return ErrorReply.INVALID_INTEGER;
        }

        if (data.length < 2 + numKeys) {
            return ErrorReply.SYNTAX;
        }

        int limit = 0;
        if (data.length > numKeys + 2) {
            if (data.length != numKeys + 2 + 2) {
                return ErrorReply.SYNTAX;
            }
            var tmpBytes = data[numKeys + 2];
            var tmp = new String(tmpBytes).toLowerCase();
            if (!"limit".equals(tmp)) {
                return ErrorReply.SYNTAX;
            }

            var limitBytes = data[numKeys + 3];
            try {
                limit = Integer.parseInt(new String(limitBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            if (limit < 0) {
                return ErrorReply.INVALID_INTEGER;
            }
        }

        for (int i = 2, j = 0; j < numKeys; i++, j++) {
            var keyBytes = data[i];
            if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
        }

        var first = slotWithKeyHashListParsed.getFirst();
        var rz = getRedisZSet(first.rawKey().getBytes(), first);
        if (rz == null || rz.isEmpty()) {
            return IntegerReply.REPLY_0;
        }

        if (!isCrossRequestWorker) {
            var memberMap = rz.getMemberMap();

            outer:
            for (int i = 1; i < slotWithKeyHashListParsed.size(); i++) {
                var other = slotWithKeyHashListParsed.get(i);
                var otherRz = getRedisZSet(other.rawKey().getBytes(), other);
                if (otherRz == null || otherRz.size() == 0) {
                    return IntegerReply.REPLY_0;
                }

                var it = rz.getSet().iterator();
                while (it.hasNext()) {
                    var sv = it.next();
                    var otherSv = otherRz.get(sv.member());
                    if (otherSv == null) {
                        it.remove();
                        memberMap.remove(sv.member());

                        if (memberMap.isEmpty()) {
                            break outer;
                        }
                    }
                }

                if (limit != 0 && memberMap.size() >= limit) {
                    break;
                }
            }

            var n = limit == 0 ? rz.size() : Math.min(rz.size(), limit);
            return n == 0 ? IntegerReply.REPLY_0 : new IntegerReply(n);
        }

        ArrayList<Promise<RedisZSet>> promises = new ArrayList<>(slotWithKeyHashListParsed.size() - 1);
        for (int i = 1; i < slotWithKeyHashListParsed.size(); i++) {
            var other = slotWithKeyHashListParsed.get(i);

            var oneSlot = localPersist.oneSlot(other.slot());
            var p = oneSlot.asyncCall(() -> getRedisZSet(other.rawKey().getBytes(), other));
            promises.add(p);
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        int finalLimit = limit;
        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("zintercard error={}", e.getMessage());
                finalPromise.setException(e);
                return;
            }

            ArrayList<RedisZSet> otherRzList = new ArrayList<>(slotWithKeyHashListParsed.size() - 1);
            for (var promise : promises) {
                otherRzList.add(promise.getResult());
            }

            var memberMap = rz.getMemberMap();

            outer:
            for (var otherRz : otherRzList) {
                if (otherRz == null || otherRz.size() == 0) {
                    finalPromise.set(IntegerReply.REPLY_0);
                    return;
                }

                var it = rz.getSet().iterator();
                while (it.hasNext()) {
                    var sv = it.next();
                    var otherSv = otherRz.get(sv.member());
                    if (otherSv == null) {
                        it.remove();
                        memberMap.remove(sv.member());

                        if (memberMap.isEmpty()) {
                            break outer;
                        }
                    }
                }

                if (finalLimit != 0 && memberMap.size() >= finalLimit) {
                    break;
                }
            }

            var n = finalLimit == 0 ? rz.size() : Math.min(rz.size(), finalLimit);
            var rr = n == 0 ? IntegerReply.REPLY_0 : new IntegerReply(n);

            finalPromise.set(rr);
        });

        return asyncReply;
    }

    @VisibleForTesting
    Reply zmscore() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var memberBytesArr = new byte[data.length - 2][];
        for (int i = 0; i < data.length - 2; i++) {
            var memberBytes = data[2 + i];
            if (memberBytes.length > RedisZSet.ZSET_MEMBER_MAX_LENGTH) {
                return ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG;
            }
            memberBytesArr[i] = memberBytes;
        }

        var replies = new Reply[memberBytesArr.length];

        var rz = getRedisZSet(keyBytes, slotWithKeyHashListParsed.getFirst());
        if (rz == null || rz.isEmpty()) {
            for (int i = 0; i < replies.length; i++) {
                replies[i] = NilReply.INSTANCE;
            }
            return new MultiBulkReply(replies);
        }

        int i = 0;
        for (var memberBytes : memberBytesArr) {
            var sv = rz.get(new String(memberBytes));
            if (sv == null) {
                replies[i++] = NilReply.INSTANCE;
            } else {
                replies[i++] = new BulkReply(String.valueOf(sv.score()).getBytes());
            }
        }
        return new MultiBulkReply(replies);
    }

    @VisibleForTesting
    Reply zpopmax(boolean isMin) {
        if (data.length != 2 && data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        int count = 1;
        if (data.length == 3) {
            var countBytes = data[2];
            try {
                count = Integer.parseInt(new String(countBytes));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            if (count <= 0) {
                return ErrorReply.INVALID_INTEGER;
            }
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var slot = slotWithKeyHash.slot();

        var rz = getRedisZSet(keyBytes, slotWithKeyHash);
        if (rz == null || rz.isEmpty()) {
            return MultiBulkReply.EMPTY;
        }

        var replies = new Reply[Math.min(count, rz.size()) * 2];
        for (int j = 0; j < replies.length; j += 2) {
            var sv = isMin ? rz.pollFirst() : rz.pollLast();
            replies[j] = new BulkReply(sv.member().getBytes());
            replies[j + 1] = new BulkReply(String.valueOf(sv.score()).getBytes());
        }

        saveRedisZSet(rz, keyBytes, slotWithKeyHash);
        return new MultiBulkReply(replies);
    }

    @VisibleForTesting
    Reply zrandmember() {
        if (data.length < 2 || data.length > 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        boolean withScores = "withscores".equalsIgnoreCase(new String(data[data.length - 1]));
        if (withScores) {
            if (data.length != 4) {
                return ErrorReply.SYNTAX;
            }
        } else {
            if (data.length == 4) {
                return ErrorReply.SYNTAX;
            }
        }

        int count = 1;
        boolean hasCount = false;
        if (data.length > 2) {
            var countBytes = data[2];
            var tmp = new String(countBytes).toLowerCase();
            try {
                count = Integer.parseInt(tmp);
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            if (count == 0) {
                return ErrorReply.INVALID_INTEGER;
            }
            hasCount = true;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rz = getRedisZSet(keyBytes, slotWithKeyHash);
        if (rz == null || rz.isEmpty()) {
            return hasCount ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }

        int size = rz.size();
        if (count > size) {
            count = size;
        }

        int absCount = Math.abs(count);

        ArrayList<Integer> indexes = HGroup.getRandIndex(count, size, absCount);

        var replies = new Reply[absCount * (withScores ? 2 : 1)];

        int j = 0;
        for (var sv : rz.getSet()) {
            for (int k = 0; k < indexes.size(); k++) {
                Integer index = indexes.get(k);
                if (index != null && index == j) {
                    replies[k * (withScores ? 2 : 1)] = new BulkReply(sv.member().getBytes());
                    if (withScores) {
                        replies[k * 2 + 1] = new BulkReply(String.valueOf(sv.score()).getBytes());
                    }
                    indexes.set(k, null);

                    boolean isAllNull = true;
                    for (int i = 0; i < indexes.size(); i++) {
                        Integer tmpIndex = indexes.get(i);
                        if (tmpIndex != null) {
                            isAllNull = false;
                            break;
                        }
                    }
                    if (isAllNull) {
                        break;
                    }
                }
            }
            j++;
        }

        return new MultiBulkReply(replies);
    }

    @VisibleForTesting
    Reply zrange(byte[][] dd) {
        return zrange(dd, null);
    }

    @VisibleForTesting
    Reply zrange(byte[][] dd, byte[] dstKeyBytes) {
        if (dd.length < 4) {
            return ErrorReply.FORMAT;
        }

        var doStore = dstKeyBytes != null;

        var keyBytes = dd[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var minBytes = dd[2];
        var maxBytes = dd[3];
        var minStr = new String(minBytes).toLowerCase();
        var maxStr = new String(maxBytes).toLowerCase();

        boolean byIndex = true;
        boolean byScore = false;
        boolean byLex = false;
        boolean isReverse = false;
        boolean hasLimit = false;
        int offset = 0;
        int count = 0;
        boolean withScores = false;

        for (int i = 4; i < dd.length; i++) {
            var tmpBytes = dd[i];
            var tmp = new String(tmpBytes).toLowerCase();
            if ("byscore".equals(tmp)) {
                byIndex = false;
                byScore = true;
                byLex = false;
            } else if ("bylex".equals(tmp)) {
                byIndex = false;
                byScore = false;
                byLex = true;
            } else if ("rev".equals(tmp)) {
                isReverse = true;
            } else if ("limit".equals(tmp)) {
                if (i + 2 >= dd.length) {
                    return ErrorReply.SYNTAX;
                }
                var offsetBytes = dd[i + 1];
                try {
                    offset = Integer.parseInt(new String(offsetBytes));
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_INTEGER;
                }
                if (offset < 0) {
                    return ErrorReply.INVALID_INTEGER;
                }

                var countBytes = dd[i + 2];
                try {
                    count = Integer.parseInt(new String(countBytes));
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_INTEGER;
                }

                hasLimit = count != 0 || offset != 0;
                i += 2;
            } else if ("withscores".equals(tmp)) {
                withScores = true;
            } else if ("byindex".equals(tmp)) {
                // do nothing
            } else {
                return ErrorReply.SYNTAX;
            }
        }

        String minLex = null;
        String maxLex = null;
        boolean minInclusive = true;
        boolean maxInclusive = true;

        if (byLex) {
            if (minBytes[0] == '-') {
                minLex = RedisZSet.MEMBER_MIN;
                minInclusive = false;
            } else {
                if (minBytes[0] != '(' && minBytes[0] != '[') {
                    return ErrorReply.SYNTAX;
                }
                if (minBytes.length == 1) {
                    return ErrorReply.SYNTAX;
                }
                minLex = minStr.substring(1);
                if (minBytes[0] == '(') {
                    minInclusive = false;
                }
            }
            if (maxBytes[0] == '+') {
                maxLex = RedisZSet.MEMBER_MAX;
                maxInclusive = false;
            } else {
                if (maxBytes[0] != '(' && maxBytes[0] != '[') {
                    return ErrorReply.SYNTAX;
                }
                if (maxBytes.length == 1) {
                    return ErrorReply.SYNTAX;
                }
                maxLex = maxStr.substring(1);
                if (maxBytes[0] == '(') {
                    maxInclusive = false;
                }
            }

            int compareMinMax = minLex.compareTo(maxLex);
            // case - (a
            if (compareMinMax > 0 && !RedisZSet.MEMBER_MIN.equals(minLex)) {
                return doStore ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
            }
        }

        double min = 0;
        double max = 0;
        if (byScore) {
            if (negInf.equals(minStr)) {
                min = Double.NEGATIVE_INFINITY;
            } else {
                int beginIndex = 0;
                if (minBytes[0] == '(') {
                    minInclusive = false;
                    beginIndex = 1;
                } else if (minBytes[0] == '[') {
                    beginIndex = 1;
                }
                try {
                    min = Double.parseDouble(minStr.substring(beginIndex));
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }
            }

            if (inf.equals(maxStr)) {
                max = Double.POSITIVE_INFINITY;
            } else {
                int beginIndex = 0;
                if (maxBytes[0] == '(') {
                    maxInclusive = false;
                    beginIndex = 1;
                } else if (maxBytes[0] == '[') {
                    beginIndex = 1;
                }
                try {
                    max = Double.parseDouble(maxStr.substring(beginIndex));
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }
            }

            if (isReverse) {
                if (min > max) {
                    double tmp = min;
                    min = max;
                    max = tmp;

                    boolean tmp2 = minInclusive;
                    minInclusive = maxInclusive;
                    maxInclusive = tmp2;
                } else {
                    return doStore ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
                }
            } else {
                if (min > max) {
                    return doStore ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
                }
            }
        }

        int start = 0;
        int stop = -1;
        if (byIndex) {
            try {
                start = Integer.parseInt(minStr);
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            try {
                stop = Integer.parseInt(maxStr);
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rz = getRedisZSet(keyBytes, slotWithKeyHash);
        if (rz == null || rz.isEmpty()) {
            return doStore ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
        }

        int size = rz.size();
        if (count <= 0) {
            count = size;
        }

        if (byIndex) {
            if (start < 0) {
                start = size + start;
                if (start < 0) {
                    start = 0;
                }
            }
            if (stop < 0) {
                stop = size + stop;
                if (stop < 0) {
                    return doStore ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
                }
            }
            if (start >= size) {
                return doStore ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
            }
            if (stop >= size) {
                stop = size - 1;
            }
            if (start > stop) {
                return doStore ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
            }

            // for range store
            var dstRz = new RedisZSet();

            var replies = hasLimit ? new Reply[Math.min(stop - start + 1, count) * (withScores ? 2 : 1)] :
                    new Reply[(stop - start + 1) * (withScores ? 2 : 1)];

            var it = isReverse ? rz.getSet().descendingSet().iterator() : rz.getSet().iterator();
            int i = 0;
            int j = 0;
            int skip = offset;
            while (it.hasNext()) {
                var sv = it.next();
                if (j >= start && j <= stop) {
                    if (hasLimit) {
                        if (skip > 0) {
                            skip--;
                            j++;
                            continue;
                        }
                    }

                    if (doStore) {
                        dstRz.add(sv.score(), sv.member());
                    }

                    replies[i++] = new BulkReply(sv.member().getBytes());
                    if (withScores) {
                        replies[i++] = new BulkReply(String.valueOf(sv.score()).getBytes());
                    }
                    // exceed count
                    if (i >= replies.length) {
                        break;
                    }
                }
                j++;
            }

            if (doStore) {
                var dstSlotWithKeyHash = slotWithKeyHashListParsed.getLast();
                if (!isCrossRequestWorker) {
                    saveRedisZSet(dstRz, dstKeyBytes, dstSlotWithKeyHash);
                } else {
                    var dstOneSlot = localPersist.oneSlot(dstSlotWithKeyHash.slot());
                    dstOneSlot.asyncRun(() -> saveRedisZSet(dstRz, dstKeyBytes, dstSlotWithKeyHash));
                }

                return dstRz.size() == 0 ? IntegerReply.REPLY_0 : new IntegerReply(dstRz.size());
            }

            // offset is too big, replies include null
            if (i == 0) {
                return MultiBulkReply.EMPTY;
            } else if (i < replies.length) {
                replies = Arrays.copyOf(replies, i);
            }

            return new MultiBulkReply(replies);
        } else if (byLex) {
            var subMap = rz.betweenByMember(minLex, minInclusive, maxLex, maxInclusive);
            if (subMap.isEmpty()) {
                return doStore ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
            }

            var it = isReverse ? subMap.descendingMap().entrySet().iterator() : subMap.entrySet().iterator();
            if (hasLimit) {
                int skip = offset;
                while (skip > 0 && it.hasNext()) {
                    it.next();
                    it.remove();
                    skip--;
                }
            }

            if (doStore) {
                var dstRz = new RedisZSet();
                int storedCount = 0;
                // subMap can be empty
                var it2 = subMap.entrySet().iterator();
                while (it2.hasNext()) {
                    if (storedCount >= count) {
                        break;
                    }
                    var entry = it2.next();
                    dstRz.add(entry.getValue().score(), entry.getKey());
                    storedCount++;
                }

                var dstSlotWithKeyHash = slotWithKeyHashListParsed.getLast();
                if (!isCrossRequestWorker) {
                    saveRedisZSet(dstRz, dstKeyBytes, dstSlotWithKeyHash);
                } else {
                    var dstOneSlot = localPersist.oneSlot(dstSlotWithKeyHash.slot());
                    dstOneSlot.asyncRun(() -> saveRedisZSet(dstRz, dstKeyBytes, dstSlotWithKeyHash));
                }

                return dstRz.isEmpty() ? IntegerReply.REPLY_0 : new IntegerReply(dstRz.size());
            }

            if (subMap.isEmpty()) {
                return MultiBulkReply.EMPTY;
            }

            var replies = hasLimit ? new Reply[Math.min(subMap.size(), count) * (withScores ? 2 : 1)] :
                    new Reply[subMap.size() * (withScores ? 2 : 1)];
            var it2 = subMap.entrySet().iterator();
            int i = 0;
            while (it2.hasNext()) {
                var entry = it2.next();
                replies[i++] = new BulkReply(entry.getKey().getBytes());
                if (withScores) {
                    replies[i++] = new BulkReply(String.valueOf(entry.getValue().score()).getBytes());
                }
                // exceed count
                if (i >= replies.length) {
                    break;
                }
            }
            return new MultiBulkReply(replies);
        } else {
            // by score
            var subSet = rz.between(min, minInclusive, max, maxInclusive);
            if (subSet.isEmpty()) {
                return doStore ? IntegerReply.REPLY_0 : MultiBulkReply.EMPTY;
            }

            var it = isReverse ? subSet.descendingSet().iterator() : subSet.iterator();
            if (hasLimit) {
                int skip = offset;
                while (skip > 0 && it.hasNext()) {
                    it.next();
                    it.remove();
                    skip--;
                }
            }

            if (doStore) {
                var dstRz = new RedisZSet();
                int storedCount = 0;
                // subSet can be empty
                var it2 = subSet.iterator();
                while (it2.hasNext()) {
                    if (storedCount >= count) {
                        break;
                    }
                    var entry = it2.next();
                    dstRz.add(entry.score(), entry.member());
                    storedCount++;
                }

                var dstSlotWithKeyHash = slotWithKeyHashListParsed.getLast();
                if (!isCrossRequestWorker) {
                    saveRedisZSet(dstRz, dstKeyBytes, dstSlotWithKeyHash);
                } else {
                    var dstOneSlot = localPersist.oneSlot(dstSlotWithKeyHash.slot());
                    dstOneSlot.asyncRun(() -> saveRedisZSet(dstRz, dstKeyBytes, dstSlotWithKeyHash));
                }

                return dstRz.isEmpty() ? IntegerReply.REPLY_0 : new IntegerReply(dstRz.size());
            }

            if (subSet.isEmpty()) {
                return MultiBulkReply.EMPTY;
            }

            var replies = hasLimit ? new Reply[Math.min(subSet.size(), count) * (withScores ? 2 : 1)] :
                    new Reply[subSet.size() * (withScores ? 2 : 1)];
            var it2 = isReverse ? subSet.descendingSet().iterator() : subSet.iterator();
            int i = 0;
            while (it2.hasNext()) {
                var entry = it2.next();
                replies[i++] = new BulkReply(entry.member().getBytes());
                if (withScores) {
                    replies[i++] = new BulkReply(String.valueOf(entry.score()).getBytes());
                }
                // exceed count
                if (i >= replies.length) {
                    break;
                }
            }
            return new MultiBulkReply(replies);
        }
    }

    @VisibleForTesting
    Reply zrank(boolean isReverse) {
        if (data.length != 3 && data.length != 4) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var memberBytes = data[2];
        if (memberBytes.length > RedisZSet.ZSET_MEMBER_MAX_LENGTH) {
            return ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG;
        }

        boolean withScore = "withscore".equalsIgnoreCase(new String(data[data.length - 1]));

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rz = getRedisZSet(keyBytes, slotWithKeyHash);
        if (rz == null || rz.isEmpty()) {
            return withScore ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }

        var sv = rz.get(new String(memberBytes));
        if (sv == null) {
            return withScore ? MultiBulkReply.EMPTY : NilReply.INSTANCE;
        }

        int rank = sv.getInitRank();
        if (isReverse) {
            rank = rz.size() - rank - 1;
        }

        if (!withScore) {
            return new IntegerReply(rank);
        }

        var replies = new Reply[2];
        replies[0] = new IntegerReply(rank);
        replies[1] = new BulkReply(String.valueOf(sv.score()).getBytes());
        return new MultiBulkReply(replies);
    }

    @VisibleForTesting
    Reply zrem() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var memberBytesArr = new byte[data.length - 2][];
        for (int i = 0; i < data.length - 2; i++) {
            var memberBytes = data[2 + i];
            if (memberBytes.length > RedisZSet.ZSET_MEMBER_MAX_LENGTH) {
                return ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG;
            }
            memberBytesArr[i] = memberBytes;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rz = getRedisZSet(keyBytes, slotWithKeyHash);
        if (rz == null || rz.isEmpty()) {
            return IntegerReply.REPLY_0;
        }

        int removedCount = 0;
        for (var memberBytes : memberBytesArr) {
            var isRemoved = rz.remove(new String(memberBytes));
            if (isRemoved) {
                removedCount++;
            }
        }

        if (removedCount > 0) {
            saveRedisZSet(rz, keyBytes, slotWithKeyHash);
        }
        return removedCount == 0 ? IntegerReply.REPLY_0 : new IntegerReply(removedCount);
    }

    @VisibleForTesting
    Reply zremrangebyscore(boolean byScore, boolean byLex, boolean byRank) {
        if (data.length != 4) {
            return ErrorReply.FORMAT;
        }

        if (!byScore && !byLex && !byRank) {
            return ErrorReply.SYNTAX;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var minBytes = data[2];
        var maxBytes = data[3];
        var minStr = new String(minBytes).toLowerCase();
        var maxStr = new String(maxBytes).toLowerCase();

        String minLex;
        String maxLex;
        boolean minInclusive = true;
        boolean maxInclusive = true;

        if (byLex) {
            if (minBytes[0] != '(' && minBytes[0] != '[') {
                return ErrorReply.SYNTAX;
            }
            if (maxBytes[0] != '(' && maxBytes[0] != '[') {
                return ErrorReply.SYNTAX;
            }

            minLex = minStr.substring(1);
            maxLex = maxStr.substring(1);

            int compareMinMax = minLex.compareTo(maxLex);
            if (compareMinMax > 0) {
                return IntegerReply.REPLY_0;
            }

            if (minBytes[0] == '(') {
                minInclusive = false;
            }
            if (maxBytes[0] == '(') {
                maxInclusive = false;
            }
        } else {
            minLex = null;
            maxLex = null;
        }

        double min = 0;
        double max = 0;
        if (byScore) {
            if (negInf.equals(minStr)) {
                min = Double.NEGATIVE_INFINITY;
            } else {
                int beginIndex = 0;
                if (minBytes[0] == '(') {
                    minInclusive = false;
                    beginIndex = 1;
                } else if (minBytes[0] == '[') {
                    beginIndex = 1;
                }
                try {
                    min = Double.parseDouble(minStr.substring(beginIndex));
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }
            }

            if (inf.equals(maxStr)) {
                max = Double.POSITIVE_INFINITY;
            } else {
                int beginIndex = 0;
                if (maxBytes[0] == '(') {
                    maxInclusive = false;
                    beginIndex = 1;
                } else if (maxBytes[0] == '[') {
                    beginIndex = 1;
                }
                try {
                    max = Double.parseDouble(maxStr.substring(beginIndex));
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_FLOAT;
                }
            }

            if (min > max) {
                return IntegerReply.REPLY_0;
            }
        }

        int start = 0;
        int stop = -1;
        if (byRank) {
            try {
                start = Integer.parseInt(minStr);
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
            try {
                stop = Integer.parseInt(maxStr);
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rz = getRedisZSet(keyBytes, slotWithKeyHash);
        if (rz == null || rz.isEmpty()) {
            return IntegerReply.REPLY_0;
        }

        if (byRank) {
            if (start < 0) {
                start = rz.size() + start;
                if (start < 0) {
                    start = 0;
                }
            }

            if (stop < 0) {
                stop = rz.size() + stop;
                if (stop < 0) {
                    return IntegerReply.REPLY_0;
                }
            }
        }

        int removed = 0;
        var memberMap = rz.getMemberMap();
        var it = rz.getSet().iterator();
        while (it.hasNext()) {
            var sv = it.next();
            if (byLex) {
                int compMin = sv.member().compareTo(minLex);
                int compMax = sv.member().compareTo(maxLex);
                if (compMin < 0 || (compMin == 0 && !minInclusive)) {
                    continue;
                }
                if (compMax > 0 || (compMax == 0 && !maxInclusive)) {
                    continue;
                }

                it.remove();
                memberMap.remove(sv.member());
                removed++;
            } else if (byScore) {
                double score = sv.score();
                if (score < min || (score == min && !minInclusive)) {
                    continue;
                }
                if (score > max || (score == max && !maxInclusive)) {
                    continue;
                }

                it.remove();
                memberMap.remove(sv.member());
                removed++;
            } else {
                int initRank = sv.getInitRank();
                if (initRank < start || initRank > stop) {
                    continue;
                }

                it.remove();
                memberMap.remove(sv.member());
                removed++;
            }
        }

        if (removed > 0) {
            saveRedisZSet(rz, keyBytes, slotWithKeyHash);
        }

        return removed == 0 ? IntegerReply.REPLY_0 : new IntegerReply(removed);
    }

    @VisibleForTesting
    Reply zscore() {
        if (data.length != 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var memberBytes = data[2];
        if (memberBytes.length > RedisZSet.ZSET_MEMBER_MAX_LENGTH) {
            return ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG;
        }

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var rz = getRedisZSet(keyBytes, slotWithKeyHash);
        if (rz == null || rz.isEmpty()) {
            return NilReply.INSTANCE;
        }

        var sv = rz.get(new String(memberBytes));
        if (sv == null) {
            return NilReply.INSTANCE;
        }

        return new BulkReply(String.valueOf(sv.score()).getBytes());
    }
}
