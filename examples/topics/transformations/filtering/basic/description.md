Basic data filtering using the `filter()` method.

The `filter()` method allows you to remove rows from your dataset based on conditions. Only rows that match the condition will be kept in the result.

```php
filter(BooleanExpression $condition): DataFrame
```

In this example, we filter the dataset to keep only rows where the `active` column is `true`. This is one of the most common operations in data processing - selecting a subset of data that meets specific criteria.

The filter expression uses Flow's DSL to reference columns and apply conditions:
- `ref('active')` - References the 'active' column
- `isTrue()` - Checks if the value is true

After filtering, only the active users (Alice, Charlie, and Diana) remain in the output.
