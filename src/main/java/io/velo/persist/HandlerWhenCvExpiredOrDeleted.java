package io.velo.persist;

import io.velo.CompressedValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Handles deletion or expiration of a compressed value.
 */
public interface HandlerWhenCvExpiredOrDeleted {
    /**
     * Invoked when a compressed value is expired or explicitly deleted.
     *
     * @param key           the key whose value expired or was deleted
     * @param shortStringCv the in-memory short string compressed value if present, otherwise null
     * @param pvm           the persisted value meta if present, otherwise null
     */
    void handleWhenCvExpiredOrDeleted(@NotNull String key, @Nullable CompressedValue shortStringCv, @Nullable PersistValueMeta pvm);
}
