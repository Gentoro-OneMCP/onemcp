package com.gentoro.onemcp.cache;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Shape object for SQL-like post-action shaping in a Prompt Schema.
 *
 * <p>According to cache_spec.txt, shape contains:
 * <ul>
 *   <li>group_by: List of field names to group by
 *   <li>aggregates: List of aggregate specifications
 *   <li>order_by: List of ordering specifications
 *   <li>limit: Optional integer limit
 *   <li>offset: Optional integer offset
 * </ul>
 *
 * <p>Shape is optional - absent if no grouping/aggregate/sorting/limiting is requested.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PromptSchemaShape {
  @JsonProperty("group_by")
  private List<String> groupBy = new ArrayList<>();

  @JsonProperty("aggregates")
  private List<AggregateSpec> aggregates = new ArrayList<>();

  @JsonProperty("order_by")
  private List<OrderSpec> orderBy = new ArrayList<>();

  @JsonProperty("limit")
  private Integer limit;

  @JsonProperty("offset")
  private Integer offset;

  public PromptSchemaShape() {}

  public List<String> getGroupBy() {
    return groupBy;
  }

  public void setGroupBy(List<String> groupBy) {
    this.groupBy = groupBy != null ? new ArrayList<>(groupBy) : new ArrayList<>();
  }

  public List<AggregateSpec> getAggregates() {
    return aggregates;
  }

  public void setAggregates(List<AggregateSpec> aggregates) {
    this.aggregates = aggregates != null ? new ArrayList<>(aggregates) : new ArrayList<>();
  }

  public List<OrderSpec> getOrderBy() {
    return orderBy;
  }

  public void setOrderBy(List<OrderSpec> orderBy) {
    this.orderBy = orderBy != null ? new ArrayList<>(orderBy) : new ArrayList<>();
  }

  public Integer getLimit() {
    return limit;
  }

  public void setLimit(Integer limit) {
    this.limit = limit;
  }

  public Integer getOffset() {
    return offset;
  }

  public void setOffset(Integer offset) {
    this.offset = offset;
  }

  /** Check if shape is empty (no shaping operations). */
  public boolean isEmpty() {
    return (groupBy == null || groupBy.isEmpty())
        && (aggregates == null || aggregates.isEmpty())
        && (orderBy == null || orderBy.isEmpty())
        && limit == null
        && offset == null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PromptSchemaShape that = (PromptSchemaShape) o;
    return Objects.equals(groupBy, that.groupBy)
        && Objects.equals(aggregates, that.aggregates)
        && Objects.equals(orderBy, that.orderBy)
        && Objects.equals(limit, that.limit)
        && Objects.equals(offset, that.offset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupBy, aggregates, orderBy, limit, offset);
  }

  @Override
  public String toString() {
    return "PromptSchemaShape{"
        + "groupBy="
        + groupBy
        + ", aggregates="
        + aggregates
        + ", orderBy="
        + orderBy
        + ", limit="
        + limit
        + ", offset="
        + offset
        + '}';
  }

  /**
   * Aggregate specification.
   *
   * <p>According to cache_spec.txt, allowed functions are built-in:
   * sum, avg, count, min, max, median, stddev, variance
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class AggregateSpec {
    @JsonProperty("field")
    private String field;

    @JsonProperty("function")
    private String function;

    public AggregateSpec() {}

    public AggregateSpec(String field, String function) {
      this.field = field;
      this.function = function;
    }

    public String getField() {
      return field;
    }

    public void setField(String field) {
      this.field = field;
    }

    public String getFunction() {
      return function;
    }

    public void setFunction(String function) {
      this.function = function;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AggregateSpec that = (AggregateSpec) o;
      return Objects.equals(field, that.field) && Objects.equals(function, that.function);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field, function);
    }

    @Override
    public String toString() {
      return "AggregateSpec{" + "field='" + field + '\'' + ", function='" + function + '\'' + '}';
    }
  }

  /**
   * Order specification.
   *
   * <p>According to cache_spec.txt, direction is "asc" or "desc".
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class OrderSpec {
    @JsonProperty("field")
    private String field;

    @JsonProperty("direction")
    private String direction; // "asc" or "desc"

    public OrderSpec() {}

    public OrderSpec(String field, String direction) {
      this.field = field;
      this.direction = direction;
    }

    public String getField() {
      return field;
    }

    public void setField(String field) {
      this.field = field;
    }

    public String getDirection() {
      return direction;
    }

    public void setDirection(String direction) {
      this.direction = direction;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      OrderSpec that = (OrderSpec) o;
      return Objects.equals(field, that.field) && Objects.equals(direction, that.direction);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field, direction);
    }

    @Override
    public String toString() {
      return "OrderSpec{" + "field='" + field + '\'' + ", direction='" + direction + '\'' + '}';
    }
  }
}




