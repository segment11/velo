package io.velo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Mark parameter object can do update after assignment.
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.PARAMETER})
public @interface ParamMutable {
}
