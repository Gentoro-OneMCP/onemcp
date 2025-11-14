package com.gentoro.onemcp.messages;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface FieldDoc {
  /** Example content or placeholder for the field. */
  String example() default "";

  /** A natural-language explanation of what this field represents. */
  String description() default "";

  /** Whether the field is required. */
  boolean required() default false;

  /** If true, the example or description should be rendered as a YAML block (|). */
  boolean multiline() default false;

  /** Optional regex or validation rule hint. */
  String pattern() default "";
}
