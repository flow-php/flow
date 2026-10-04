`isValid()` asks the same question as `assert()` without throwing, so it can be used in a condition.
It checks the value as it is: `'42'` is not an integer, and JSON text is not `json` until `cast()`
turns it into a `Json` value.
