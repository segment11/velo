package io.velo.command;

/**
 * Utility class for index start/end range validation and normalization.
 */
public class IndexStartEndReset {
    private IndexStartEndReset() {
    }

    /**
     * Represents a validated start/end range with validity flag.
     *
     * @param start the start index (inclusive)
     * @param end   the end index (inclusive)
     * @param valid true if the range is valid
     */
    public record StartEndWithValidFlag(int start, int end, boolean valid) {
        static final StartEndWithValidFlag INVALID = new StartEndWithValidFlag(0, 0, false);
    }

    /**
     * Resets and validates start/end indices for array indexing.
     *
     * @param start       the start index (negative values offset from end)
     * @param end         the end index (negative values offset from end)
     * @param valueLength the length of the value array
     * @return StartEndWithValidFlag containing validated indices and validity status
     */
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
