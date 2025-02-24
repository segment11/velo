package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.Dict;
import io.velo.repl.ReplContent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * Represents the content of a REPL message of type {@link io.velo.repl.ReplType#s_exists_dict}.
 * This message is used to send information about dictionaries from a master to a slave.
 * It includes a map of dictionaries that need to be sent, excluding those that have already been sent.
 */
public class ToSlaveExistsDict implements ReplContent {
    private final HashMap<Dict, String> cacheKeyByDict;
    private final TreeMap<Integer, Dict> toSendCacheDictBySeq;

    /**
     * Constructs a new {@link ToSlaveExistsDict} message.
     *
     * @param cacheDict       A map of UUID strings to dictionaries that exist in the cache.
     * @param cacheDictBySeq  A map of sequence numbers to dictionaries that exist in the cache.
     * @param sentDictSeqList A list of sequence numbers of dictionaries that have already been sent to the slave.
     */
    public ToSlaveExistsDict(HashMap<String, Dict> cacheDict, TreeMap<Integer, Dict> cacheDictBySeq, ArrayList<Integer> sentDictSeqList) {
        var cacheKeyByDict = new HashMap<Dict, String>();
        for (var entry : cacheDict.entrySet()) {
            cacheKeyByDict.put(entry.getValue(), entry.getKey());
        }
        this.cacheKeyByDict = cacheKeyByDict;

        var toSendCacheDictBySeq = new TreeMap<Integer, Dict>();
        // Exclude dictionaries that have already been sent
        for (var entry : cacheDictBySeq.entrySet()) {
            if (!sentDictSeqList.contains(entry.getKey())) {
                toSendCacheDictBySeq.put(entry.getKey(), entry.getValue());
            }
        }
        this.toSendCacheDictBySeq = toSendCacheDictBySeq;
    }

    /**
     * Encodes the content of this message to the given {@link ByteBuf}.
     * <p>
     * The encoding format is as follows:
     * - First 4 bytes: the count of dictionaries to send.
     * - For each dictionary to send:
     * - 4 bytes: the length of the encoded dictionary.
     * - Following bytes: the encoded dictionary.
     *
     * @param toBuf The buffer to which the encoded message content will be written.
     */
    @Override
    public void encodeTo(ByteBuf toBuf) {
        if (toSendCacheDictBySeq.isEmpty()) {
            toBuf.writeInt(0);
            return;
        }

        toBuf.writeInt(toSendCacheDictBySeq.size());

        for (var entry : toSendCacheDictBySeq.entrySet()) {
            var dict = entry.getValue();
            var key = cacheKeyByDict.get(dict);

            var encodeLength = dict.encodeLength(key);
            toBuf.writeInt(encodeLength);
            toBuf.write(dict.encode(key));
        }
    }

    /**
     * Returns the length of the encoded message content in bytes.
     * <p>
     * The length is calculated as follows:
     * - 4 bytes for the count of dictionaries to send.
     * - For each dictionary to send:
     * - 4 bytes for the length of the encoded dictionary.
     * - The length of the encoded dictionary.
     *
     * @return The length of the encoded message content in bytes.
     */
    @Override
    public int encodeLength() {
        if (toSendCacheDictBySeq.isEmpty()) {
            return 4;
        }

        var length = 4;
        for (var entry : toSendCacheDictBySeq.entrySet()) {
            var dict = entry.getValue();
            var key = cacheKeyByDict.get(dict);

            var encodeLength = dict.encodeLength(key);
            length += 4 + encodeLength;
        }
        return length;
    }
}