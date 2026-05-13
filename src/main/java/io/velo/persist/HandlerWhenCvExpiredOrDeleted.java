package io.velo.persist;

import io.velo.CompressedValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Handles deletion or expiration of a compressed value.
 */
public interface HandlerWhenCvExpiredOrDeleted {
    void handleWhenCvExpiredOrDeleted(@NotNull String key, @Nullable CompressedValue shortStringCv, @Nullable PersistValueMeta pvm);
}
