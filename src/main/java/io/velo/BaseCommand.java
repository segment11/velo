package io.velo;

import com.github.luben.zstd.Zstd;
import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.acl.AclUsers;
import io.velo.acl.U;
import io.velo.command.AGroup;
import io.velo.decode.Request;
import io.velo.mock.ByPassGetSet;
import io.velo.persist.KeyLoader;
import io.velo.persist.LocalPersist;
import io.velo.persist.OneSlot;
import io.velo.repl.cluster.MultiShard;
import io.velo.reply.Reply;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.util.JedisClusterCRC16;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Abstract base class for handling Velo commands.
 * This class provides core functionalities for parsing command data, handling slots, and interacting with the persistence layer.
 */
@ThreadNeedLocal
public abstract class BaseCommand {
    /**
     * Functional interface to determine if an index in the command data array contains a key.
     */
    protected interface IsKeyBytes {
        boolean isKeyBytes(int i);
    }

    /**
     * Implementation of IsKeyBytes that checks if a specific index contains a key.
     */
    protected static final class GivenKeyIndex implements IsKeyBytes {
        private final int index;

        public GivenKeyIndex(int index) {
            this.index = index;
        }

        @Override
        public boolean isKeyBytes(int i) {
            return i == index;
        }
    }

    /**
     * Implementation of IsKeyBytes that checks if indices within a range contain keys.
     */
    protected static final class FromToKeyIndex implements IsKeyBytes {
        private final int from;
        private final int to;
        private final int step;

        public FromToKeyIndex(int from, int to, int step) {
            this.from = from;
            this.to = to;
            this.step = step;
        }

        @Override
        public boolean isKeyBytes(int i) {
            return i >= from && (to == -1 || i <= to) && (step == 1 || (i - from) % step == 0);
        }
    }

    /**
     * Predefined IsKeyBytes instances for common use cases.
     */
    protected static final IsKeyBytes KeyIndex1 = new GivenKeyIndex(1);
    protected static final IsKeyBytes KeyIndex2 = new GivenKeyIndex(2);
    protected static final IsKeyBytes KeyIndex1And2 = new FromToKeyIndex(1, 2, 1);
    protected static final IsKeyBytes KeyIndexBegin1 = new FromToKeyIndex(1, -1, 1);
    protected static final IsKeyBytes KeyIndexBegin1Step2 = new FromToKeyIndex(1, -1, 2);
    protected static final IsKeyBytes KeyIndexBegin2 = new FromToKeyIndex(2, -1, 1);
    protected static final IsKeyBytes KeyIndexBegin2Step2 = new FromToKeyIndex(2, -1, 2);

    /**
     * Adds slot-key hash mappings for relevant keys in the command data array.
     *
     * @param slotWithKeyHashList The list to store slot-key hash mappings
     * @param data                All data array received from the client including command
     * @param slotNumber          Total number of slots in the velo running instance
     * @param isKeyBytes          Predicate to determine which array indices contain keys
     */
    protected static void addToSlotWithKeyHashList(ArrayList<SlotWithKeyHash> slotWithKeyHashList,
                                                   byte[][] data, int slotNumber, IsKeyBytes isKeyBytes) {
        for (int i = 1; i < data.length; i++) {
            if (isKeyBytes.isKeyBytes(i)) {
                var keyBytes = data[i];
                var slotWithKeyHash = slot(keyBytes, slotNumber);
                slotWithKeyHashList.add(slotWithKeyHash);
            }
        }
    }

    /**
     * Gets the command string received from the client.
     *
     * @return The command string received from the client
     */
    public String getCmd() {
        return cmd;
    }

    /**
     * Gets all data array received from the client including command.
     *
     * @return All data array received from the client including command
     */
    public byte[][] getData() {
        return data;
    }

    /**
     * Gets the network socket connection for the command.
     *
     * @return The TCP socket connection to the client
     */
    public ITcpSocket getSocket() {
        return socket;
    }

    // need final, for unit test, can change
    protected String cmd;
    protected byte[][] data;
    protected ITcpSocket socket;

    @TestOnly
    public void setCmd(String cmd) {
        this.cmd = cmd;
    }

    @TestOnly
    public void setData(byte[][] data) {
        this.data = data;
    }

    @TestOnly
    public void setSocket(ITcpSocket socket) {
        this.socket = socket;
    }

    /**
     * Constructs a BaseCommand instance with the provided command string, data array, and network socket connection.
     *
     * @param cmd    The command string received from the client
     * @param data   All data array received from the client including command
     * @param socket The TCP socket connection to the client
     */
    public BaseCommand(String cmd, byte[][] data, ITcpSocket socket) {
        this.cmd = cmd;
        this.data = data;
        this.socket = socket;
    }

    /**
     * Gets the Acl user object associated with the network socket connection.
     *
     * @param socket0 The network socket connection
     * @return Acl user, use a default user if not found
     */
    public static @NotNull U getAuthU(ITcpSocket socket0) {
        var authUser = SocketInspector.getAuthUser(socket0);
        return authUser == null ? U.INIT_DEFAULT_U : AclUsers.getInstance().get(authUser);
    }

    protected @NotNull U getAuthU() {
        return getAuthU(socket);
    }

    protected final DictMap dictMap = DictMap.getInstance();

    protected RequestHandler requestHandler;

    @TestOnly
    public void setRequestHandler(RequestHandler requestHandler) {
        this.requestHandler = requestHandler;
    }

    protected byte workerId;
    protected byte netWorkers;

    @TestOnly
    public short getSlotNumber() {
        return slotNumber;
    }

    protected short slotNumber;
    protected CompressStats compressStats;

    protected int trainSampleListMaxSize = 1000;

    @TestOnly
    public void setSnowFlake(SnowFlake snowFlake) {
        this.snowFlake = snowFlake;
    }

    protected SnowFlake snowFlake;
    protected TrainSampleJob trainSampleJob;
    protected List<TrainSampleJob.TrainSampleKV> sampleToTrainList;

    @TestOnly
    public void setLocalTest(boolean localTest) {
        this.localTest = localTest;
    }

    @TestOnly
    public void setLocalTestRandomValueList(ArrayList<byte[]> localTestRandomValueList) {
        this.localTestRandomValueList = localTestRandomValueList;
    }

    @TestOnly
    public ArrayList<byte[]> getLocalTestRandomValueList() {
        return localTestRandomValueList;
    }

    @TestOnly
    protected boolean localTest;
    @TestOnly
    protected int localTestRandomValueListSize;
    @TestOnly
    protected ArrayList<byte[]> localTestRandomValueList;

    protected ArrayList<SlotWithKeyHash> slotWithKeyHashListParsed;

    @TestOnly
    public ArrayList<SlotWithKeyHash> getSlotWithKeyHashListParsed() {
        return slotWithKeyHashListParsed;
    }

    @TestOnly
    public void setSlotWithKeyHashListParsed(ArrayList<SlotWithKeyHash> slotWithKeyHashListParsed) {
        this.slotWithKeyHashListParsed = slotWithKeyHashListParsed;
    }

    protected boolean isCrossRequestWorker;

    /**
     * Sets whether the command needs to be sent to other workers.
     * When do unit test, can change
     * When reuse a command method, can change
     *
     * @param crossRequestWorker Whether the command needs to be sent to other workers
     */
    public void setCrossRequestWorker(boolean crossRequestWorker) {
        isCrossRequestWorker = crossRequestWorker;
    }

    @TestOnly
    public static AGroup mockAGroup() {
        return BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1);
    }

    @TestOnly
    public static AGroup mockAGroup(byte workerId, byte netWorkers, short slotNumber) {
        return mockAGroup(workerId, netWorkers, slotNumber, new CompressStats("mock", "net_"),
                Zstd.defaultCompressionLevel(), 100, new SnowFlake(1, 1),
                new TrainSampleJob(workerId), new ArrayList<>(),
                false, 0, new ArrayList<>(),
                new ArrayList<>(), false);
    }

    @TestOnly
    public static AGroup mockAGroup(byte workerId, byte netWorkers, short slotNumber, CompressStats compressStats,
                                    int compressLevel, int trainSampleListMaxSize, SnowFlake snowFlake,
                                    TrainSampleJob trainSampleJob, List<TrainSampleJob.TrainSampleKV> sampleToTrainList,
                                    boolean localTest, int localTestRandomValueListSize, ArrayList<byte[]> localTestRandomValueList,
                                    ArrayList<SlotWithKeyHash> slotWithKeyHashListParsed, boolean isCrossRequestWorker) {
        var aGroup = new AGroup("append", new byte[][]{new byte[0], new byte[0], new byte[0]}, null);
        aGroup.workerId = workerId;
        aGroup.netWorkers = netWorkers;
        aGroup.slotNumber = slotNumber;

        aGroup.compressStats = compressStats;

        aGroup.trainSampleListMaxSize = trainSampleListMaxSize;

        aGroup.snowFlake = snowFlake;

        aGroup.trainSampleJob = trainSampleJob;
        aGroup.sampleToTrainList = sampleToTrainList;

        aGroup.localTest = localTest;
        aGroup.localTestRandomValueListSize = localTestRandomValueListSize;
        aGroup.localTestRandomValueList = localTestRandomValueList;

        aGroup.slotWithKeyHashListParsed = slotWithKeyHashListParsed;
        aGroup.isCrossRequestWorker = isCrossRequestWorker;
        return aGroup;
    }

    /**
     * Copy the properties of another BaseCommand object to this object.
     * When do unit test, can use this
     * When reuse a command method, can use this
     *
     * @param other The other BaseCommand object to copy properties from
     */
    public void from(BaseCommand other) {
        this.requestHandler = other.requestHandler;

        this.netWorkers = other.netWorkers;
        this.slotNumber = other.slotNumber;

        this.compressStats = other.compressStats;

        this.trainSampleListMaxSize = other.trainSampleListMaxSize;

        this.snowFlake = other.snowFlake;

        this.trainSampleJob = other.trainSampleJob;
        this.sampleToTrainList = other.sampleToTrainList;

        this.localTest = other.localTest;
        this.localTestRandomValueListSize = other.localTestRandomValueListSize;
        this.localTestRandomValueList = other.localTestRandomValueList;

        this.slotWithKeyHashListParsed = other.slotWithKeyHashListParsed;
        this.isCrossRequestWorker = other.isCrossRequestWorker;

        if (other.byPassGetSet != null) {
            this.byPassGetSet = other.byPassGetSet;
        }
    }

    /**
     * Initialize the properties of this BaseCommand object.
     *
     * @param requestHandler The RequestHandler object associated with this BaseCommand object
     * @param request        The Request object associated with this BaseCommand object
     * @return The BaseCommand object itself
     */
    public BaseCommand init(RequestHandler requestHandler, Request request) {
        this.requestHandler = requestHandler;

        this.netWorkers = requestHandler.netWorkers;
        this.slotNumber = requestHandler.slotNumber;

        this.compressStats = requestHandler.compressStats;

        this.trainSampleListMaxSize = requestHandler.trainSampleListMaxSize;

        this.snowFlake = requestHandler.snowFlake;

        this.trainSampleJob = requestHandler.trainSampleJob;
        this.sampleToTrainList = requestHandler.sampleToTrainList;

        this.localTest = requestHandler.localTest;
        this.localTestRandomValueListSize = requestHandler.localTestRandomValueListSize;
        this.localTestRandomValueList = requestHandler.localTestRandomValueList;

        this.slotWithKeyHashListParsed = request.getSlotWithKeyHashList();
        this.isCrossRequestWorker = request.isCrossRequestWorker();
        return this;
    }

    /**
     * Parse the slots hash information from the given command and data.
     *
     * @param cmd        The command string
     * @param data       All data array
     * @param slotNumber The slot number
     * @return List of SlotWithKeyHash objects representing the parsed slots
     */
    public abstract ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber);

    public abstract Reply handle();

    @TestOnly
    public Reply execute(String allDataString) {
        var dataStrings = allDataString.split(" ");
        var data = new byte[dataStrings.length][];
        for (int i = 0; i < dataStrings.length; i++) {
            data[i] = dataStrings[i].getBytes();
        }

        this.cmd = dataStrings[0];
        this.data = data;

        slotWithKeyHashListParsed = parseSlots(cmd, data, slotNumber);
        return handle();
    }

    protected final LocalPersist localPersist = LocalPersist.getInstance();

    // create log for each object, perf bad, use static better
    protected static final Logger log = LoggerFactory.getLogger(BaseCommand.class);

    /**
     * Represents a key's slot assignment and hash information.
     *
     * @param slot         The internal slot index used for sharding
     * @param toClientSlot The slot number exposed to Redis clients when cluster enabled
     * @param bucketIndex  The bucket index within the slot
     * @param keyHash      Main 64-bit hash of the key
     * @param keyHash32    Another 32 bits of the key
     * @param rawKey       The original key string
     */
    public record SlotWithKeyHash(short slot, short toClientSlot, int bucketIndex,
                                  long keyHash, int keyHash32, String rawKey) {
        @Override
        public String toString() {
            return "SlotWithKeyHash{" +
                    "slot=" + slot +
                    ", toClientSlot=" + toClientSlot +
                    ", bucketIndex=" + bucketIndex +
                    ", keyHash=" + keyHash +
                    ", rawKey='" + rawKey + '\'' +
                    '}';
        }

        public static final short IGNORE_TO_CLIENT_SLOT = -1;

        public SlotWithKeyHash(short slot, int bucketIndex, long keyHash, int keyHash32, String rawKey) {
            this(slot, IGNORE_TO_CLIENT_SLOT, bucketIndex, keyHash, keyHash32, rawKey);
        }

        public SlotWithKeyHash(short slot, int bucketIndex, long keyHash, int keyHash32) {
            this(slot, IGNORE_TO_CLIENT_SLOT, bucketIndex, keyHash, keyHash32, null);
        }

        public static final SlotWithKeyHash TO_FIX_FIRST_SLOT = new SlotWithKeyHash((short) 0, 0, 0L, 0, null);
    }

    @VisibleForTesting
    static long tagHash(byte[] keyBytes) {
        int hashTagBeginIndex = -1;
        int hashTagEndIndex = -1;
        for (int i = 0; i < keyBytes.length; i++) {
            if (keyBytes[i] == '{') {
                hashTagBeginIndex = i;
            } else if (keyBytes[i] == '}') {
                hashTagEndIndex = i;
            }
        }

        if (hashTagBeginIndex >= 0 && hashTagEndIndex > hashTagBeginIndex) {
            // hash tag
            return KeyHash.hashOffset(keyBytes, hashTagBeginIndex + 1, hashTagEndIndex - hashTagBeginIndex - 1);
        }

        return 0L;
    }

    /**
     * For one redis client connection, can send commands by keys, and Velo server can always do not cross worker (thread), for perf better.
     *
     * @param keyBytes       The key bytes
     * @param slotNumber     Velo server slot number
     * @param bucketsPerSlot Velo server configure buckets per slot
     * @return The slot number for the given key
     */
    public static short calSlotInRedisClientWhenNeedBetterPerf(byte[] keyBytes, int slotNumber, int bucketsPerSlot) {
        final int halfSlotNumber = slotNumber / 2;
        final int x = halfSlotNumber * bucketsPerSlot;

        var tagHash = tagHash(keyBytes);
        if (tagHash != 0L) {
            var slotPositive = slotNumber == 1 ? 0 : Math.abs((tagHash / x) & (halfSlotNumber - 1));
            var slot = tagHash > 0 ? slotPositive : halfSlotNumber + slotPositive;
            return (short) slot;
        }

        var keyHash = KeyHash.hash(keyBytes);
        var slotPositive = slotNumber == 1 ? 0 : Math.abs((keyHash / x) & (halfSlotNumber - 1));
        var slot = tagHash > 0 ? slotPositive : halfSlotNumber + slotPositive;
        return (short) slot;
    }

    /**
     * Calculates slot assignment and hash information for a key.
     *
     * @param keyBytes   Original key bytes
     * @param slotNumber Total number of slots in the velo running instance
     * @return SlotWithKeyHash containing full positioning information
     */
    public static SlotWithKeyHash slot(byte[] keyBytes, int slotNumber) {
        var bucketsPerSlot = ConfForSlot.global.confBucket.bucketsPerSlot;

        var keyHash = KeyHash.hash(keyBytes);
        var keyHash32 = KeyHash.hash32(keyBytes);
        // bucket index always use xxhash 64
        var bucketIndex = Math.abs(keyHash & (bucketsPerSlot - 1));

        if (ConfForGlobal.clusterEnabled) {
            // use crc16
            var toClientSlot = JedisClusterCRC16.getSlot(keyBytes);
            var innerSlot = MultiShard.asInnerSlotByToClientSlot(toClientSlot);
            return new SlotWithKeyHash(innerSlot, (short) toClientSlot, (int) bucketIndex, keyHash, keyHash32, new String(keyBytes));
        }

        final int halfSlotNumber = slotNumber / 2;
        final int x = halfSlotNumber * bucketsPerSlot;

        var tagHash = tagHash(keyBytes);
        if (tagHash != 0L) {
            var slotPositive = slotNumber == 1 ? 0 : Math.abs((tagHash / x) & (halfSlotNumber - 1));
            var slot = tagHash > 0 ? slotPositive : halfSlotNumber + slotPositive;
            return new SlotWithKeyHash((short) slot, (int) bucketIndex, keyHash, keyHash32, new String(keyBytes));
        }

        var slotPositive = slotNumber == 1 ? 0 : Math.abs((keyHash / x) & (halfSlotNumber - 1));
        var slot = keyHash > 0 ? slotPositive : halfSlotNumber + slotPositive;
        return new SlotWithKeyHash((short) slot, (int) bucketIndex, keyHash, keyHash32, new String(keyBytes));
    }

    /**
     * Calculates slot assignment for a key using instance-configured slot number.
     *
     * @param keyBytes Original key bytes
     * @return SlotWithKeyHash Key hash information including slot number, bucket index, and key hash.
     */
    public SlotWithKeyHash slot(byte[] keyBytes) {
        return slot(keyBytes, slotNumber);
    }

    // for mock test
    private ByPassGetSet byPassGetSet;

    @TestOnly
    public void setByPassGetSet(ByPassGetSet byPassGetSet) {
        this.byPassGetSet = byPassGetSet;
    }

    /**
     * Gets expiration time for a key in milliseconds since epoch.
     *
     * @param keyBytes        Original key bytes
     * @param slotWithKeyHash Precomputed slot and hash information
     * @return Expiration time or null if no expiration set
     */
    public Long getExpireAt(byte[] keyBytes, @NotNull SlotWithKeyHash slotWithKeyHash) {
        if (byPassGetSet != null) {
            var cv = getCv(keyBytes, slotWithKeyHash);
            if (cv == null) {
                return null;
            }
            return cv.getExpireAt();
        } else {
            var slot = slotWithKeyHash.slot();
            var oneSlot = localPersist.oneSlot(slot);
            return oneSlot.getExpireAt(keyBytes, slotWithKeyHash.bucketIndex, slotWithKeyHash.keyHash, slotWithKeyHash.keyHash32);
        }
    }

    /**
     * Gets the CompressedValue for a key and precomputed slot and hash information.
     *
     * @param keyBytes Original key bytes
     * @param s        Precomputed slot and hash information
     * @return CompressedValue or null if expired/not found
     * @throws DictMissingException If required compression dictionary is missing
     */
    public CompressedValue getCv(byte[] keyBytes, SlotWithKeyHash s) {
        var slot = s.slot();

        OneSlot.BufOrCompressedValue bufOrCompressedValue;
        if (byPassGetSet != null) {
            bufOrCompressedValue = byPassGetSet.getBuf(slot, keyBytes, s.bucketIndex, s.keyHash);
        } else {
            var oneSlot = localPersist.oneSlot(slot);
            bufOrCompressedValue = oneSlot.get(keyBytes, s.bucketIndex, s.keyHash, s.keyHash32);
        }

        if (bufOrCompressedValue == null) {
            return null;
        }

        var cv = bufOrCompressedValue.cv() != null ? bufOrCompressedValue.cv() :
                CompressedValue.decode(bufOrCompressedValue.buf(), keyBytes, s.keyHash());
        if (cv.isExpired()) {
            return null;
        }

        if (cv.isBigString()) {
            var oneSlot = localPersist.oneSlot(slot);

            var buffer = ByteBuffer.wrap(cv.getCompressedData());
            var uuid = buffer.getLong();
            var realDictSeq = buffer.getInt();

            var bigStringBytes = oneSlot.getBigStringFiles().getBigStringBytes(uuid, true);
            if (bigStringBytes == null) {
                return null;
            }

            cv.setCompressedData(bigStringBytes);
            cv.setCompressedLength(bigStringBytes.length);
            cv.setDictSeqOrSpType(realDictSeq);
        }

        return cv;
    }

    @TestOnly
    byte[] getValueBytesByCv(CompressedValue cv) {
        return getValueBytesByCv(cv, null, null);
    }

    /**
     * Get value bytes from a CompressedValue.
     *
     * @param cv              The CompressedValue to decompress
     * @param keyBytes        Original key bytes
     * @param slotWithKeyHash Precomputed slot and hash information
     * @return Decompressed value bytes
     * @throws DictMissingException If required compression dictionary is missing
     */
    public byte[] getValueBytesByCv(CompressedValue cv, byte[] keyBytes, SlotWithKeyHash slotWithKeyHash) {
        if (cv.isTypeNumber()) {
            return String.valueOf(cv.numberValue()).getBytes();
        }

        if (cv.isCompressed()) {
            Dict dict = null;
            if (cv.isUseDict()) {
                var dictSeqOrSpType = cv.getDictSeqOrSpType();
                if (dictSeqOrSpType == Dict.SELF_ZSTD_DICT_SEQ) {
                    dict = Dict.SELF_ZSTD_DICT;
                } else if (dictSeqOrSpType == Dict.GLOBAL_ZSTD_DICT_SEQ) {
                    if (!Dict.GLOBAL_ZSTD_DICT.hasDictBytes()) {
                        throw new DictMissingException("Global dict bytes not set");
                    }
                    dict = Dict.GLOBAL_ZSTD_DICT;
                } else {
                    dict = dictMap.getDictBySeq(dictSeqOrSpType);
                    if (dict == null) {
                        throw new DictMissingException("Dict not found, dict seq=" + dictSeqOrSpType);
                    }
                }
            }

            var beginT = System.nanoTime();
            var decompressed = cv.decompress(dict);
            var costT = System.nanoTime() - beginT;

            // stats
            compressStats.decompressedCount++;
            compressStats.decompressedCostTimeTotalNs += costT;

            if (slotWithKeyHash != null && byPassGetSet == null) {
                localPersist.oneSlot(slotWithKeyHash.slot).monitorBigKeyByValueLength(keyBytes, decompressed.length);
            }
            return decompressed;
        } else {
            var compressedData = cv.getCompressedData();
            if (slotWithKeyHash != null && byPassGetSet == null) {
                localPersist.oneSlot(slotWithKeyHash.slot).monitorBigKeyByValueLength(keyBytes, compressedData.length);
            }
            return compressedData;
        }
    }

    /**
     * Get value bytes from a key.
     *
     * @param keyBytes        Original key bytes
     * @param slotWithKeyHash Precomputed slot and hash information
     * @return Value bytes or null if not found
     */
    public byte[] get(byte[] keyBytes, SlotWithKeyHash slotWithKeyHash) {
        return get(keyBytes, slotWithKeyHash, false);
    }

    private static boolean contain(Integer[] expectSpTypeArray, int spType) {
        for (var expectSpType : expectSpTypeArray) {
            if (expectSpType == spType) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get value bytes from a key.
     *
     * @param keyBytes          Original key bytes
     * @param slotWithKeyHash   Precomputed slot and hash information
     * @param expectTypeString  Expect return CompressValue is string type or ignore
     * @param expectSpTypeArray Expect return CompressValue sp type array
     * @return Value bytes or null if not found
     */
    public byte[] get(byte[] keyBytes, SlotWithKeyHash slotWithKeyHash, boolean expectTypeString, Integer... expectSpTypeArray) {
        var cv = getCv(keyBytes, slotWithKeyHash);
        if (cv == null) {
            return null;
        }
        if (expectTypeString && !cv.isTypeString()) {
            throw new TypeMismatchException("Expect type string, but got sp type=" + cv.getDictSeqOrSpType());
        }
        if (expectSpTypeArray.length > 0 && !contain(expectSpTypeArray, cv.getDictSeqOrSpType())) {
            throw new TypeMismatchException("Expect sp type array=" + Arrays.toString(expectSpTypeArray) + ", but got sp type=" + cv.getDictSeqOrSpType());
        }
        return getValueBytesByCv(cv, keyBytes, slotWithKeyHash);
    }

    /**
     * Set a number value to a key.
     *
     * @param keyBytes             Original key bytes
     * @param value                Number value
     * @param slotWithKeyHashReuse Precomputed slot and hash information
     */
    public void setNumber(byte[] keyBytes, Number value, SlotWithKeyHash slotWithKeyHashReuse) {
        setNumber(keyBytes, value, slotWithKeyHashReuse, CompressedValue.NO_EXPIRE);
    }

    /**
     * Set a number value to a key.
     *
     * @param keyBytes             Original key bytes
     * @param value                Number value
     * @param slotWithKeyHashReuse Precomputed slot and hash information
     * @param expireAt             Given expire time
     */
    public void setNumber(byte[] keyBytes, Number value, SlotWithKeyHash slotWithKeyHashReuse, long expireAt) {
        if (value instanceof Short) {
            short shortValue = value.shortValue();
            if (shortValue <= Byte.MAX_VALUE && shortValue >= Byte.MIN_VALUE) {
                var newValueBytes = new byte[1];
                newValueBytes[0] = (byte) shortValue;
                set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_BYTE, expireAt);
            } else {
                var newValueBytes = new byte[2];
                ByteBuffer.wrap(newValueBytes).putShort(shortValue);
                set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_SHORT, expireAt);
            }
        } else if (value instanceof Integer) {
            int intValue = value.intValue();
            if (intValue <= Byte.MAX_VALUE && intValue >= Byte.MIN_VALUE) {
                var newValueBytes = new byte[1];
                newValueBytes[0] = (byte) intValue;
                set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_BYTE, expireAt);
            } else if (intValue <= Short.MAX_VALUE && intValue >= Short.MIN_VALUE) {
                var newValueBytes = new byte[2];
                ByteBuffer.wrap(newValueBytes).putShort((short) intValue);
                set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_SHORT, expireAt);
            } else {
                var newValueBytes = new byte[4];
                ByteBuffer.wrap(newValueBytes).putInt(intValue);
                set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_INT, expireAt);
            }
        } else if (value instanceof Long) {
            long longValue = value.longValue();
            if (longValue <= Byte.MAX_VALUE && longValue >= Byte.MIN_VALUE) {
                var newValueBytes = new byte[1];
                newValueBytes[0] = (byte) longValue;
                set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_BYTE, expireAt);
            } else if (longValue <= Short.MAX_VALUE && longValue >= Short.MIN_VALUE) {
                var newValueBytes = new byte[2];
                ByteBuffer.wrap(newValueBytes).putShort((short) longValue);
                set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_SHORT, expireAt);
            } else if (longValue <= Integer.MAX_VALUE && longValue >= Integer.MIN_VALUE) {
                var newValueBytes = new byte[4];
                ByteBuffer.wrap(newValueBytes).putInt((int) longValue);
                set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_INT, expireAt);
            } else {
                var newValueBytes = new byte[8];
                ByteBuffer.wrap(newValueBytes).putLong(longValue);
                set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_LONG, expireAt);
            }
        } else if (value instanceof Double) {
            double doubleValue = value.doubleValue();
            var newValueBytes = new byte[8];
            ByteBuffer.wrap(newValueBytes).putDouble(doubleValue);
            set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_DOUBLE, expireAt);
        } else {
            throw new IllegalArgumentException("Not support number type=" + value.getClass());
        }
    }

    /**
     * Stores a CompressedValue in the persistence layer.
     *
     * @param keyBytes             Original key bytes
     * @param cv                   CompressedValue to store
     * @param slotWithKeyHashReuse Precomputed slot and hash information
     */
    public void setCv(byte[] keyBytes, CompressedValue cv, SlotWithKeyHash slotWithKeyHashReuse) {
        var slotWithKeyHash = slotWithKeyHashReuse != null ? slotWithKeyHashReuse : slot(keyBytes);
        if (cv.isTypeNumber()) {
            setNumber(keyBytes, cv.numberValue(), slotWithKeyHash);
        } else {
            // update to new seq
            cv.setSeq(snowFlake.nextId());
            // update key hash
            cv.setKeyHash(slotWithKeyHash.keyHash());

            if (cv.isCompressed()) {
                // just set cv
                var dstKey = new String(keyBytes);
                var slot = slotWithKeyHash.slot();

                putToOneSlot(slot, dstKey, slotWithKeyHash, cv);

                var indexHandlerPool = localPersist.getIndexHandlerPool();
                if (indexHandlerPool != null) {
                    var shortType = KeyLoader.transferToShortType(cv.getDictSeqOrSpType());
                    var valueLengthHigh24WithShortTypeLow8 = cv.getUncompressedLength() << 8 | shortType;
                    indexHandlerPool.getKeyAnalysisHandler().addKey(slotWithKeyHash.rawKey, valueLengthHigh24WithShortTypeLow8);
                }
            } else {
                set(keyBytes, cv.getCompressedData(), slotWithKeyHash, 0, cv.getExpireAt());
            }
        }
    }

    @TestOnly
    public void set(byte[] keyBytes, byte[] valueBytes) {
        set(keyBytes, valueBytes, slot(keyBytes, slotNumber), CompressedValue.NULL_DICT_SEQ, CompressedValue.NO_EXPIRE);
    }

    /**
     * Stores a value in the persistence layer.
     *
     * @param keyBytes             Original key bytes
     * @param valueBytes           Value bytes
     * @param slotWithKeyHashReuse Precomputed slot and hash information
     */
    public void set(byte[] keyBytes, byte[] valueBytes, @NotNull SlotWithKeyHash slotWithKeyHashReuse) {
        set(keyBytes, valueBytes, slotWithKeyHashReuse, CompressedValue.NULL_DICT_SEQ, CompressedValue.NO_EXPIRE);
    }

    /**
     * Stores a value in the persistence layer.
     *
     * @param keyBytes        Original key bytes
     * @param valueBytes      Value bytes
     * @param slotWithKeyHash Precomputed slot and hash information
     * @param spType          Given as CompressValue spType
     */
    public void set(byte[] keyBytes, byte[] valueBytes, @NotNull SlotWithKeyHash slotWithKeyHash, int spType) {
        set(keyBytes, valueBytes, slotWithKeyHash, spType, CompressedValue.NO_EXPIRE);
    }

    private static final int MAX_LONG_VALUE_IN_BYTES_LENGTH = String.valueOf(Long.MAX_VALUE).length();

    /**
     * Stores a value in the persistence layer.
     *
     * @param keyBytes        Original key bytes
     * @param valueBytes      Value bytes
     * @param slotWithKeyHash Precomputed slot and hash information
     * @param spType          Given as CompressValue spType
     * @param expireAt        Given expire time
     */
    public void set(byte[] keyBytes, byte[] valueBytes, @NotNull SlotWithKeyHash slotWithKeyHash, int spType, long expireAt) {
        compressStats.rawTotalLength += valueBytes.length;

        // prefer store as number type
        boolean isTypeNumber = CompressedValue.isTypeNumber(spType);
        if (valueBytes.length <= MAX_LONG_VALUE_IN_BYTES_LENGTH && !isTypeNumber) {
            var value = new String(valueBytes);

            // check if value is a number
            long longValue;
            try {
                longValue = Long.parseLong(value);
                setNumber(keyBytes, longValue, slotWithKeyHash, expireAt);
                return;
            } catch (NumberFormatException ignore) {
            }

            double doubleValue;
            try {
                doubleValue = Double.parseDouble(value);
                setNumber(keyBytes, doubleValue, slotWithKeyHash, expireAt);
                return;
            } catch (NumberFormatException ignore) {
            }
        }

        var key = new String(keyBytes);
        var preferDoCompress = valueBytes.length >= DictMap.TO_COMPRESS_MIN_DATA_LENGTH;
        var preferDoCompressUseSelfDict = valueBytes.length >= DictMap.TO_COMPRESS_USE_SELF_DICT_MIN_DATA_LENGTH;

        Dict dict = null;
        boolean isTypeString = CompressedValue.isTypeString(spType);
        if (isTypeString && ConfForGlobal.isValueSetUseCompression && preferDoCompress) {
            // use global dict first
            if (Dict.GLOBAL_ZSTD_DICT.hasDictBytes()) {
                dict = Dict.GLOBAL_ZSTD_DICT;
            } else {
                // use trained dict if key prefix or suffix match
                var keyPrefixOrSuffix = TrainSampleJob.keyPrefixOrSuffixGroup(key);
                dict = dictMap.getDict(keyPrefixOrSuffix);

                if (dict == null && preferDoCompressUseSelfDict) {
                    // use self dict
                    dict = Dict.SELF_ZSTD_DICT;
                }
            }
        }

        var slot = slotWithKeyHash.slot;
        if (ConfForGlobal.isValueSetUseCompression && preferDoCompress && dict != null) {
            var beginT = System.nanoTime();
            // dict may be null
            var cv = CompressedValue.compress(valueBytes, dict);
            var costT = (System.nanoTime() - beginT) / 1000;
            cv.setSeq(snowFlake.nextId());
            if (cv.isIgnoreCompression(valueBytes)) {
                cv.setDictSeqOrSpType(CompressedValue.NULL_DICT_SEQ);
            } else {
                cv.setDictSeqOrSpType(dict.getSeq());
            }
            cv.setKeyHash(slotWithKeyHash.keyHash);
            cv.setExpireAt(expireAt);

            putToOneSlot(slot, key, slotWithKeyHash, cv);

            // stats
            compressStats.compressedCount++;
            compressStats.compressedTotalLength += cv.getCompressedLength();
            compressStats.compressedCostTimeTotalUs += costT;

            if (ConfForGlobal.isOnDynTrainDictForCompression) {
                if (dict == Dict.SELF_ZSTD_DICT) {
                    // add train sample list
                    if (sampleToTrainList.size() < trainSampleListMaxSize) {
                        var kv = new TrainSampleJob.TrainSampleKV(key, null, cv.getSeq(), valueBytes);
                        sampleToTrainList.add(kv);
                    } else {
                        // no async train, latency sensitive
                        trainSampleJob.resetSampleToTrainList(sampleToTrainList);
                        handleTrainSampleResult(trainSampleJob.train());
                    }
                }
            }
        } else {
            var cvRaw = new CompressedValue();
            cvRaw.setSeq(snowFlake.nextId());
            cvRaw.setDictSeqOrSpType(spType);
            cvRaw.setKeyHash(slotWithKeyHash.keyHash);
            cvRaw.setExpireAt(expireAt);
            cvRaw.setUncompressedLength(valueBytes.length);
            cvRaw.setCompressedLength(valueBytes.length);
            cvRaw.setCompressedData(valueBytes);

            putToOneSlot(slot, key, slotWithKeyHash, cvRaw);

            if (ConfForGlobal.isValueSetUseCompression && ConfForGlobal.isOnDynTrainDictForCompression) {
                // add train sample list
                if (sampleToTrainList.size() < trainSampleListMaxSize) {
                    if (preferDoCompress) {
                        var kv = new TrainSampleJob.TrainSampleKV(key, null, cvRaw.getSeq(), valueBytes);
                        sampleToTrainList.add(kv);
                    }
                } else {
                    // no async train, latency sensitive
                    trainSampleJob.resetSampleToTrainList(sampleToTrainList);
                    handleTrainSampleResult(trainSampleJob.train());
                }

                // stats
                compressStats.rawCount++;
                compressStats.compressedTotalLength += valueBytes.length;
            }
        }

        var indexHandlerPool = localPersist.getIndexHandlerPool();
        if (indexHandlerPool != null) {
            var shortType = KeyLoader.transferToShortType(spType);
            var valueLengthHigh24WithShortTypeLow8 = valueBytes.length << 8 | shortType;
            indexHandlerPool.getKeyAnalysisHandler().addKey(key, valueLengthHigh24WithShortTypeLow8);
        }
    }

    /**
     * Store a CompressedValue to a specific slot's storage.
     *
     * @param slot            Target slot number
     * @param key             Original key string
     * @param slotWithKeyHash Precomputed slot and hash information
     * @param cv              CompressedValue to store
     */
    protected void putToOneSlot(short slot, String key, @NotNull SlotWithKeyHash slotWithKeyHash, CompressedValue cv) {
        if (byPassGetSet != null) {
            byPassGetSet.put(slot, key, slotWithKeyHash.bucketIndex, cv);
        } else {
            var oneSlot = localPersist.oneSlot(slot);
            try {
                oneSlot.put(key, slotWithKeyHash.bucketIndex, cv);
            } catch (Exception e) {
                log.error("Set error, key={}, message={}", key, e.getMessage());
                throw e;
            }
        }
    }

    /**
     * Removes a key from the persistence layer.
     *
     * @param slot        Target slot number
     * @param bucketIndex Precomputed bucket index
     * @param key         The key string to remove
     * @param keyHash     Main 64-bit key hash
     * @param keyHash32   Another 32 bits of key
     * @return true if the key was found and removed
     */
    public boolean remove(short slot, int bucketIndex, String key, long keyHash, int keyHash32) {
        if (byPassGetSet != null) {
            return byPassGetSet.remove(slot, key);
        }

        var oneSlot = localPersist.oneSlot(slot);
        return oneSlot.remove(key, bucketIndex, keyHash, keyHash32);
    }

    /**
     * Schedules a key for delayed removal (asynchronous deletion).
     *
     * @param slot        Target slot number
     * @param bucketIndex Precomputed bucket index
     * @param key         The key string to remove
     * @param keyHash     Main 64-bit key hash
     */
    public void removeDelay(short slot, int bucketIndex, String key, long keyHash) {
        if (byPassGetSet != null) {
            byPassGetSet.remove(slot, key);
            return;
        }

        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.removeDelay(key, bucketIndex, keyHash);
    }

    /**
     * Checks if a key exists in the persistence layer.
     *
     * @param slot        Target slot number
     * @param bucketIndex Precomputed bucket index
     * @param key         The key string to check
     * @param keyHash     Main 64-bit key hash
     * @param keyHash32   Another 32 bits of key hash
     * @return true if the key exists and is not expired
     */
    public boolean exists(short slot, int bucketIndex, String key, long keyHash, int keyHash32) {
        if (byPassGetSet != null) {
            var bufOrCompressedValue = byPassGetSet.getBuf(slot, key.getBytes(), bucketIndex, keyHash);
            return bufOrCompressedValue != null;
        }

        var oneSlot = localPersist.oneSlot(slot);
        return oneSlot.exists(key, bucketIndex, keyHash, keyHash32);
    }

    @VisibleForTesting
    void handleTrainSampleResult(TrainSampleJob.TrainSampleResult trainSampleResult) {
        if (trainSampleResult == null) {
            return;
        }

        var trainSampleCacheDict = trainSampleResult.cacheDict();
        if (!trainSampleCacheDict.isEmpty()) {
            // train success, set dict in this worker
            for (var entry : trainSampleCacheDict.entrySet()) {
                var keyPrefix = entry.getKey();
                var dict = entry.getValue();
                dictMap.putDict(keyPrefix, dict);
//                var oldDict = dictMap.putDict(keyPrefixOrSuffix, dict);
//                if (oldDict != null) {
//                    // keep old dict in persist, because may be used by other worker
//                    // when start server, early dict will be overwritten by new dict with same key prefix, need not persist again?
//                    dictMap.putDict(keyPrefixOrSuffix + "_" + new Random().nextInt(10000), oldDict);
//                }
            }
        }

        var removedSampleKVSeqList = trainSampleResult.removedSampleKVSeqList();
        if (!removedSampleKVSeqList.isEmpty()) {
            // remove sample already trained
            sampleToTrainList.removeIf(kv -> removedSampleKVSeqList.contains(kv.seq()));
        }
    }
}
