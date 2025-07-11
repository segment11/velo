package io.velo.repl;

import java.io.File;

public interface ActAsFile {
    int fileIndex();

    boolean delete();

    long length();

    String getName();

    class PersistFile implements ActAsFile {
        final File file;
        private final int fileIndex;

        public PersistFile(File file, int fileIndex) {
            this.file = file;
            this.fileIndex = fileIndex;
        }

        @Override
        public int fileIndex() {
            return fileIndex;
        }

        @Override
        public boolean delete() {
            return file.delete();
        }

        @Override
        public long length() {
            return file.length();
        }

        @Override
        public String getName() {
            return file.getName();
        }
    }
}
