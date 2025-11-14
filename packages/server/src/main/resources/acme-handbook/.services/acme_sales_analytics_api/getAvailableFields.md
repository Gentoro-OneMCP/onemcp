# getAvailableFields
----------------------------------------
| Property        | Value              |
|-----------------|--------------------|
| Method          | GET |
| Relative Path   | /fields   |
----------------------------------------
      
```md
Get Available Fields

Retrieve a list of all available fields in the sales dataset.
This endpoint helps you understand what data is available for querying.


```

### Response `200`
List of available fields

```json5
Object({
  // (Optional): 
  "fields": Array(
    Object({
      // (Optional): Full field name (e.g., "customer.name")
      "name": String,
      // (Optional): Data type of the field
      "type": String(Enum("string", "number", "boolean", "date", "datetime")),
      // (Optional): Human-readable description of the field
      "description": String,
      // (Optional): Category this field belongs to
      "category": String,
      // (Optional): Whether this field can contain null values
      "nullable": Boolean,
      // (Optional): Example value for this field
      "example": OneOf(String, Number, Boolean)
    })
  ),
  // (Optional): 
  "categories": Array(
    Object({
      // (Optional): 
      "name": String,
      // (Optional): 
      "fields": Array(
        String
      )
    })
  )
})
```

