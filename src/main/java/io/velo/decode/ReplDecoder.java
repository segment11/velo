package io.velo.decode;

import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.decoder.ByteBufsDecoder;
import io.velo.repl.Repl;
import io.velo.repl.ReplRequest;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * A decoder for incoming requests that can handle velo replication protocols.
 * The decoder reads from a {@link ByteBufs} input and attempts to parse as many complete requests as possible.
 */
public class ReplDecoder implements ByteBufsDecoder<ArrayList<ReplRequest>> {
    private final HashMap<ByteBufs, ReplRequest> toFullyReadRequests = new HashMap<>();

    /**
     * Attempts to decode a single request from the provided ByteBufs.
     *
     * @param bufs the source of bytes to decode
     * @return a decoded replication data bytes or null if there aren't enough bytes to form a complete request
     */
    private ReplRequest tryDecodeOne(ByteBufs bufs) {
        var buf = RequestDecoder.fromBufs(bufs);
        if (buf == null) {
            return null;
        }

        var toFullyReadOne = toFullyReadRequests.get(bufs);
        if (toFullyReadOne != null) {
            var leftN = toFullyReadOne.leftToRead();
            var canReadN = buf.readableBytes();
            if (canReadN >= leftN) {
                toFullyReadOne.nextRead(buf, leftN);
            } else {
                toFullyReadOne.nextRead(buf, canReadN);
            }

            int consumedN = buf.readerIndex();
            bufs.skip(consumedN);

            if (toFullyReadOne.isFullyRead()) {
                toFullyReadRequests.remove(bufs);
                return toFullyReadOne;
            } else {
                return null;
            }
        }

        var replRequest = Repl.decode(buf);
        // data not all ready
        if (replRequest == null) {
            return null;
        }

        // Skip the bytes those were consumed during decoding
        int consumedN = buf.readerIndex();
        bufs.skip(consumedN);

        if (replRequest.isFullyRead()) {
            toFullyReadRequests.remove(bufs);
        } else {
            toFullyReadRequests.put(bufs, replRequest);
        }

        return replRequest;
    }

    /**
     * Attempts to decode multiple requests from the provided ByteBufs until no more complete requests can be formed.
     *
     * @param bufs the source of bytes to decode
     * @return a list of decoded repl data bytes
     * @throws MalformedDataException if the data in ByteBufs is malformed and cannot be decoded
     */
    @Override
    public @NotNull ArrayList<ReplRequest> tryDecode(ByteBufs bufs) throws MalformedDataException {
        try {
            ArrayList<ReplRequest> pipeline = new ArrayList<>();
            var one = tryDecodeOne(bufs);
            while (one != null && one.isFullyRead()) {
                pipeline.add(one);
                one = tryDecodeOne(bufs);
            }
            return pipeline;
        } catch (Exception e) {
            throw new MalformedDataException(e);
        }
    }
}