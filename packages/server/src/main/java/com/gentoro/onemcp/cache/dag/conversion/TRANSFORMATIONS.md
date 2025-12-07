# Value Transformation Reference

This document describes all available transformations for converting conceptual values to API values in the DAG execution system.

## Transformation Types

### Type Conversions

#### String to Number
- **`str_to_int`**: Convert string to integer
  - Example: `"2024"` → `2024`
  - Example: `"123.45"` → `123` (truncates decimal)
  
- **`str_to_long`**: Convert string to long integer
  - Example: `"9223372036854775807"` → `9223372036854775807L`
  
- **`str_to_double`**: Convert string to double
  - Example: `"123.45"` → `123.45`
  
- **`str_to_float`**: Convert string to float
  - Example: `"123.45"` → `123.45f`

#### Number to String
- **`int_to_str`**: Convert integer to string
  - Example: `2024` → `"2024"`
  
- **`double_to_str`**: Convert double to string
  - Example: `123.45` → `"123.45"`

#### Boolean Conversions
- **`str_to_bool`**: Convert string to boolean
  - Accepts: `"true"`, `"1"`, `"yes"`, `"on"` → `true`
  - All other values → `false`
  
- **`bool_to_str`**: Convert boolean to string
  - Example: `true` → `"true"`, `false` → `"false"`
  
- **`bool_to_int`**: Convert boolean to integer
  - Example: `true` → `1`, `false` → `0`

### String Operations

- **`uppercase`**: Convert to uppercase
  - Example: `"hello"` → `"HELLO"`
  
- **`lowercase`**: Convert to lowercase
  - Example: `"HELLO"` → `"hello"`
  
- **`trim`**: Remove leading/trailing whitespace
  - Example: `"  hello  "` → `"hello"`
  
- **`trim_to_empty`**: Trim, return empty string if null
  - Example: `null` → `""`, `"  hello  "` → `"hello"`
  
- **`trim_to_null`**: Trim, return null if empty
  - Example: `"  "` → `null`, `"  hello  "` → `"hello"`
  
- **`capitalize`**: Capitalize first letter
  - Example: `"hello"` → `"Hello"`
  
- **`uncapitalize`**: Lowercase first letter
  - Example: `"Hello"` → `"hello"`
  
- **`swap_case`**: Swap case of all characters
  - Example: `"Hello World"` → `"hELLO wORLD"`

### Regex Transformations

Regex transformations require pattern and replacement parameters:

- **`regex_replace`**: Replace all matches with replacement
  - Pattern: regex pattern (Java regex syntax)
  - Replacement: replacement string (can use `$1`, `$2` for groups)
  - Example: pattern=`"\\d+"`, replacement=`"NUM"`, value=`"abc123def"` → `"abcNUMdef"`
  
- **`regex_extract`**: Extract first matching group
  - Pattern: regex pattern with capturing groups
  - Returns: first capturing group, or full match if no groups
  - Example: pattern=`"\\d+"`, value=`"abc123def"` → `"123"`

### Number Formatting

- **`format_decimal`**: Format number, remove trailing zeros
  - Example: `123.0` → `"123"`, `123.45` → `"123.45"`

### Null/Empty Handling

- **`default_empty`**: Return empty string if null
  - Example: `null` → `""`
  
- **`default_null`**: Return null if empty string
  - Example: `""` → `null`

## Usage in DAG

Transformations are specified in ConvertValue nodes:

```json
{
  "id": "cv1",
  "type": "ConvertValue",
  "config": {
    "conceptualFieldKind": "date_yyyy",
    "targetFormat": "year_int",
    "value": "$._initial.v1",
    "transformations": [
      "str_to_int"
    ]
  }
}
```

Or using the transformation registry directly:

```java
TransformationRegistry registry = new TransformationRegistry();
Object result = registry.apply("str_to_int", "2024"); // Returns 2024
```

## Libraries Used

- **Apache Commons Lang 3**: String and number utilities (`StringUtils`, `NumberUtils`)
- **Java Regex**: Pattern matching and replacement (`java.util.regex.Pattern`, `Matcher`)
- **Java Time API**: Date/time conversions (already in use)

## Extending Transformations

To add custom transformations:

```java
TransformationRegistry registry = new TransformationRegistry();
registry.register("custom_transform", value -> {
  // Your transformation logic
  return transformedValue;
});
```

