package io.velo.persist;

import io.velo.CompressedValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Interface for handling the deletion or expiration of a key value pair.
 */
public interface HandlerWhenCvExpiredOrDeleted {
    void handleWhenCvExpiredOrDeleted(@NotNull String key, @Nullable CompressedValue shortStringCv, @Nullable PersistValueMeta pvm);
}
