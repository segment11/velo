package io.velo.persist;

import org.jetbrains.annotations.NotNull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Interface for saving and loading.
 */
public interface CanSaveAndLoad {
    /**
     * Load from last saved file when pure memory.
     *
     * @param is the input stream
     * @throws IOException when io exception
     */
    void loadFromLastSavedFileWhenPureMemory(@NotNull DataInputStream is) throws IOException;

    /**
     * Write to saved file when pure memory.
     *
     * @param os the output stream
     * @throws IOException when io exception
     */
    void writeToSavedFileWhenPureMemory(@NotNull DataOutputStream os) throws IOException;
}
