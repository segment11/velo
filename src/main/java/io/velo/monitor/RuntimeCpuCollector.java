package io.velo.monitor;

import oshi.SystemInfo;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

/**
 * Collects runtime CPU process information using OSHI.
 */
public class RuntimeCpuCollector {
    private static final OperatingSystem os = new SystemInfo().getOperatingSystem();

    /** Closes the collector and prints a success message. */
    public static void close() {
        System.out.println("Runtime cpu collector close success");
    }

    /** @return the current process information */
    public static OSProcess collect() {
        return os.getProcess(os.getProcessId());
    }
}
