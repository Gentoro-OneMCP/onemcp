package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * A single filter expression in a Prompt Schema filter array.
 *
 * <p>Represents a pre-action filter condition applied to narrow the set of entities before the
 * action is executed.
 *
 * <p>According to cache_spec.txt, allowed operators are built-in (not from lexicon):
 * <ul>
 *   <li>equals, not_equals
 *   <li>greater_than, greater_than_or_equal, less_than, less_than_or_equal
 *   <li>contains, not_contains, starts_with, ends_with
 *   <li>in, not_in, between
 *   <li>is_null, is_not_null
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FilterExpression {
  @JsonProperty("field")
  private String field;

  @JsonProperty("operator")
  private String operator;

  @JsonProperty("value")
  private Object value; // Primitive or array of primitives (for 'in', 'between')

  public FilterExpression() {}

  public FilterExpression(String field, String operator, Object value) {
    this.field = field;
    this.operator = operator;
    this.value = value;
  }

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }

  public String getOperator() {
    return operator;
  }

  public void setOperator(String operator) {
    this.operator = operator;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FilterExpression that = (FilterExpression) o;
    return Objects.equals(field, that.field)
        && Objects.equals(operator, that.operator)
        && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, operator, value);
  }

  @Override
  public String toString() {
    return "FilterExpression{"
        + "field='"
        + field
        + '\''
        + ", operator='"
        + operator
        + '\''
        + ", value="
        + value
        + '}';
  }
}




