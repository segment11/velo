package io.velo.command;

public class IndexStartEndReset {
    private IndexStartEndReset() {
    }

    public record StartEndWithValidFlag(int start, int end, boolean valid) {
        static final StartEndWithValidFlag INVALID = new StartEndWithValidFlag(0, 0, false);
    }

    public static StartEndWithValidFlag reset(int start, int end, int valueLength) {
        if (start < 0) {
            start = valueLength + start;
            if (start < 0) {
                start = 0;
            }
        }
        if (end < 0) {
            end = valueLength + end;
            if (end < 0) {
                return StartEndWithValidFlag.INVALID;
            }
        }
        if (start >= valueLength) {
            return StartEndWithValidFlag.INVALID;
        }
        if (end >= valueLength) {
            end = valueLength - 1;
        }
        if (start > end) {
            return StartEndWithValidFlag.INVALID;
        }

        return new StartEndWithValidFlag(start, end, true);
    }
}
