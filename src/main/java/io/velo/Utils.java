package io.velo;

import java.io.File;

public class Utils {
    private Utils() {
    }

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

    public static String projectPath(String relativePath) {
        return workDir + relativePath;
    }
}
