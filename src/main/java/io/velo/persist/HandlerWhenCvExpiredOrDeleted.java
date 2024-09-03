package io.velo.persist;

import io.velo.CompressedValue;

public interface HandlerWhenCvExpiredOrDeleted {
    void handleWhenCvExpiredOrDeleted(String key, CompressedValue shortStringCv, PersistValueMeta pvm);
}
