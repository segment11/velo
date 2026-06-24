package io.velo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Mark one class or field is need to be thread local.
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.TYPE, ElementType.FIELD})
public @interface ThreadNeedLocal {
    /** The kind of thread-local binding this class or field needs, e.g. {@code "slot"}. */
    String type() default "slot";
}
