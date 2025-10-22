Basic usage of the `when` transformation function.

The `when` function provides conditional logic within data transformations. It evaluates a condition and returns different values based on whether the condition is true or false.

```php
when(
    condition: BooleanExpression,
    then: mixed,
    else: mixed
): mixed
```

In this example, we use `when` to add a new entry `is_special` that checks if a row is both active AND contains the 'foo' tag.
