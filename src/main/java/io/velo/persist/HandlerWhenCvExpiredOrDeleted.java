package io.velo.persist;

import io.velo.CompressedValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface HandlerWhenCvExpiredOrDeleted {
    void handleWhenCvExpiredOrDeleted(@NotNull String key, @Nullable CompressedValue shortStringCv, @Nullable PersistValueMeta pvm);
}
