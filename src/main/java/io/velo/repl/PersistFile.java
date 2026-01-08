package io.velo.repl;

import java.io.File;

record PersistFile(File file, int fileIndex) {
    public boolean delete() {
        return file.delete();
    }

    public long length() {
        return file.length();
    }

    public String getName() {
        return file.getName();
    }
}