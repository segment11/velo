package io.velo.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

/**
 * Collects runtime CPU process information using OSHI.
 */
public class RuntimeCpuCollector {
    private static final Logger log = LoggerFactory.getLogger(RuntimeCpuCollector.class);

    private static final OperatingSystem os = new SystemInfo().getOperatingSystem();

    /** Closes the collector. */
    public static void close() {
        log.info("Runtime cpu collector closed");
    }

    /** @return the current process information */
    public static OSProcess collect() {
        return os.getProcess(os.getProcessId());
    }
}
