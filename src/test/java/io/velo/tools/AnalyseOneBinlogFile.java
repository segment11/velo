package io.velo.tools;

import io.velo.repl.Binlog;

import java.io.File;
import java.io.IOException;

public class AnalyseOneBinlogFile {
    public static void main(String[] args) throws IOException {
        var binlogFilePath = args.length > 0 ? args[0] : "/tmp/velo/persist/slot-0/binlog/binlog-0";
        var binlogFile = new File(binlogFilePath);
        if (!binlogFile.exists()) {
            System.out.println("binlog file not exists");
            return;
        }

        Binlog.analyseBinlogFile(binlogFile);
    }
}
