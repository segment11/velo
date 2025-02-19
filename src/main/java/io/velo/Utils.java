package io.velo;

import java.io.File;
import java.util.Random;

/**
 * Utility class providing common utility methods.
 */
public class Utils {
    private Utils() {
        // Private constructor to prevent instantiation
    }

    /**
     * Pads the right side of the string with the specified padding string until the total length of the string reaches the specified length.
     *
     * @param s   The original string.
     * @param pad The string to use for padding.
     * @param len The desired total length of the string.
     * @return The right-padded string.
     */
    public static String rightPad(String s, String pad, int len) {
        if (s.length() >= len) {
            return s;
        }
        StringBuilder sb = new StringBuilder(s);
        while (sb.length() < len) {
            sb.append(pad);
        }
        return sb.toString();
    }

    /**
     * Pads the left side of the string with the specified padding string until the total length of the string reaches the specified length.
     *
     * @param s   The original string.
     * @param pad The string to use for padding.
     * @param len The desired total length of the string.
     * @return The left-padded string.
     */
    public static String leftPad(String s, String pad, int len) {
        if (s.length() >= len) {
            return s;
        }
        var sb = new StringBuilder();
        while (sb.length() < len - s.length()) {
            sb.append(pad);
        }
        sb.append(s);
        return sb.toString();
    }

    /**
     * The working directory of the project. This is determined based on the current path of the application.
     */
    private static final String workDir;

    static {
        var currentPath = new File(".").getAbsolutePath().replaceAll("\\\\", "/");
        // if run in IDE
        var pos = currentPath.indexOf("/src/io/velo");
        if (pos != -1) {
            workDir = currentPath.substring(0, pos);
        } else {
            // if run in jar
            workDir = currentPath;
        }
    }

    /**
     * Returns the absolute path to a file or directory relative to the project's root directory.
     *
     * @param relativePath The relative path to the file or directory.
     * @return The absolute path to the file or directory.
     */
    public static String projectPath(String relativePath) {
        return workDir + relativePath;
    }

    /**
     * Character array containing digits (0-9) and lowercase letters (a-z).
     */
    private static final char[] CHAR_ARRAY = new char[10 + 26];

    static {
        // Initialize CHAR_ARRAY with digits
        for (int i = 0; i < 10; i++) {
            CHAR_ARRAY[i] = (char) ('0' + i);
        }
        // Initialize CHAR_ARRAY with lowercase letters
        for (int i = 0; i < 26; i++) {
            CHAR_ARRAY[i + 10] = (char) ('a' + i);
        }
    }

    /**
     * Generates a random string of specified length consisting of digits and lowercase letters.
     *
     * @param length the length of the random string to generate
     * @return a random string of the specified length
     * @throws IllegalArgumentException if the length is negative
     */
    public static String generateRandomChars(int length) {
        if (length < 0) {
            throw new IllegalArgumentException("Length must be non-negative");
        }
        Random random = new Random();
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(CHAR_ARRAY[random.nextInt(CHAR_ARRAY.length)]);
        }
        return sb.toString();
    }
}