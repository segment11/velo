package io.velo.tool.pcap.extend

import groovy.transform.CompileStatic

@CompileStatic
class ConsoleColorExtensions {
    private ConsoleColorExtensions() {}

    @CompileStatic
    static enum Color {
        red(31), yellow(32), orange(33), blue(34), purple(35), green(36)

        int n

        Color(int n) {
            this.n = n
        }
    }

    static String color(String self, Color color) {
        String.format("\033[%dm%s\033[0m", color ? color.n : Color.red.n, self)
    }

    static String red(String self) {
        color(self, Color.red)
    }

    static String yellow(String self) {
        color(self, Color.yellow)
    }

    static String orange(String self) {
        color(self, Color.orange)
    }

    static String blue(String self) {
        color(self, Color.blue)
    }

    static String purple(String self) {
        color(self, Color.purple)
    }

    static String green(String self) {
        color(self, Color.green)
    }
}
