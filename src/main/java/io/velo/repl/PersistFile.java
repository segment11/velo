package io.velo.repl;

import java.io.File;

/**
 * Persisted file wrapper with file index.
 *
 * @param file      the file
 * @param fileIndex the file index
 */
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