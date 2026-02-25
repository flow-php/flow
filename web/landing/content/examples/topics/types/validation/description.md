Type validation checks if a value matches the expected type without throwing exceptions. The `isValid()` method returns `true` if the value conforms to the type, `false` otherwise. This is useful for conditional logic, filtering data, or performing pre-flight checks before processing.

Unlike assertions, validation never throws - it's designed for control flow decisions rather than enforcing contracts.
