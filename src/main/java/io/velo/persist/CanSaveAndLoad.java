package io.velo.persist;

import org.jetbrains.annotations.NotNull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface CanSaveAndLoad {
    void loadFromLastSavedFileWhenPureMemory(@NotNull DataInputStream is) throws IOException;

    void writeToSavedFileWhenPureMemory(@NotNull DataOutputStream os) throws IOException;
}
