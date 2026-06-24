package io.velo.repl;

import java.io.File;

/**
 * Persisted file wrapper with file index.
 *
 * @param file      the file
 * @param fileIndex the file index
 */
record PersistFile(File file, int fileIndex) {
    /**
     * Deletes the underlying file.
     *
     * @return true if the file was successfully deleted, false otherwise
     */
    public boolean delete() {
        return file.delete();
    }

    /**
     * Gets the length of the underlying file in bytes.
     *
     * @return the length of the file in bytes
     */
    public long length() {
        return file.length();
    }

    /**
     * Gets the name of the underlying file.
     *
     * @return the name of the file
     */
    public String getName() {
        return file.getName();
    }
}