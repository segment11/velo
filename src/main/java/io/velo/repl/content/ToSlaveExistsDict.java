package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.Dict;
import io.velo.repl.ReplContent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

public class ToSlaveExistsDict implements ReplContent {
    private final HashMap<Dict, String> cacheKeyByDict;
    private final TreeMap<Integer, Dict> toSendCacheDictBySeq;

    public ToSlaveExistsDict(HashMap<String, Dict> cacheDict, TreeMap<Integer, Dict> cacheDictBySeq, ArrayList<Integer> sentDictSeqList) {
        var cacheKeyByDict = new HashMap<Dict, String>();
        for (var entry : cacheDict.entrySet()) {
            cacheKeyByDict.put(entry.getValue(), entry.getKey());
        }
        this.cacheKeyByDict = cacheKeyByDict;

        var toSendCacheDictBySeq = new TreeMap<Integer, Dict>();
        // exclude sent dict
        for (var entry : cacheDictBySeq.entrySet()) {
            if (!sentDictSeqList.contains(entry.getKey())) {
                toSendCacheDictBySeq.put(entry.getKey(), entry.getValue());
            }
        }
        this.toSendCacheDictBySeq = toSendCacheDictBySeq;
    }

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
