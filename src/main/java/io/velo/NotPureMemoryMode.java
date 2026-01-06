package io.velo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks fields or methods that are only used when the application is not in pure memory mode.
 * This annotation is used to identify code paths that are specific to disk-based persistence
 * and should be excluded or handled differently when running in pure memory mode.
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.TYPE, ElementType.FIELD, ElementType.METHOD})
public @interface NotPureMemoryMode {
}
