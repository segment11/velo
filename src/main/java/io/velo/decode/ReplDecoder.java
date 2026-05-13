package io.velo.decode;

import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.decoder.ByteBufsDecoder;
import io.velo.repl.Repl;
import io.velo.repl.ReplRequest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;

/**
 * Decoder for incoming Velo replication protocol requests.
 */
public class ReplDecoder implements ByteBufsDecoder<ArrayList<ReplRequest>> {
    // slave tcp client is only one in one slot, one repl pair
    @VisibleForTesting
    ReplRequest toFullyReadRequest;

    /**
     * @param bufs the source of bytes to decode
     * @return a decoded ReplRequest or null if not enough bytes
     */
    private ReplRequest tryDecodeOne(ByteBufs bufs) {
        var buf = RequestDecoder.fromBufs(bufs);
        if (buf == null) {
            return null;
        }

        if (toFullyReadRequest != null) {
            var leftN = toFullyReadRequest.leftToRead();
            var canReadN = buf.readableBytes();
            if (canReadN >= leftN) {
                toFullyReadRequest.nextRead(buf, leftN);
            } else {
                toFullyReadRequest.nextRead(buf, canReadN);
            }

            int consumedN = buf.readerIndex();
            bufs.skip(consumedN);

            if (toFullyReadRequest.isFullyRead()) {
                var one = toFullyReadRequest.copyShadow();
                toFullyReadRequest = null;
                return one;
            } else {
                return toFullyReadRequest;
            }
        }

        var replRequest = Repl.decode(buf);
        // invalid repl request, or not ready
        if (replRequest == null) {
            return null;
        }

        // skip the bytes those were consumed during decoding
        int consumedN = buf.readerIndex();
        bufs.skip(consumedN);

        if (!replRequest.isFullyRead()) {
            assert toFullyReadRequest == null;
            toFullyReadRequest = replRequest;
        }

        return replRequest;
    }

    /**
     * @param bufs the source of bytes to decode
     * @return a list of decoded ReplRequest objects
     */
    @Override
    public @NotNull ArrayList<ReplRequest> tryDecode(ByteBufs bufs) throws MalformedDataException {
        try {
            ArrayList<ReplRequest> pipeline = new ArrayList<>();
            var one = tryDecodeOne(bufs);
            while (one != null) {
                if (one.isFullyRead()) {
                    pipeline.add(one);
                }
                one = tryDecodeOne(bufs);
            }
            return pipeline;
        } catch (Exception e) {
            throw new MalformedDataException(e);
        }
    }
}