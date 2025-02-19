package io.velo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Mark parameter as nullable only for test.
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.PARAMETER})
public @interface NullableOnlyTest {
}

