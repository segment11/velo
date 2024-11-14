package io.velo.util;

public class Utils {
    private static final char[] CHAR_ARRAY = new char[10 + 26];

    static {
        for (int i = 0; i < 10; i++) {
            CHAR_ARRAY[i] = (char) ('0' + i);
        }
        for (int i = 0; i < 26; i++) {
            CHAR_ARRAY[i + 10] = (char) ('a' + i);
        }
    }

    public static String generateRandomChars(int length) {
        var random = new java.util.Random();
        var sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(CHAR_ARRAY[random.nextInt(CHAR_ARRAY.length)]);
        }
        return sb.toString();
    }
}
