package io.velo.monitor;

import oshi.SystemInfo;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

public class RuntimeCpuCollector {
    private static final OperatingSystem os = new SystemInfo().getOperatingSystem();

    public static void close() {
        System.out.println("Runtime cpu collector close success");
    }

    public static OSProcess collect() {
        return os.getProcess(os.getProcessId());
    }
}
