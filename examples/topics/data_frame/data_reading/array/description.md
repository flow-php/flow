Read data directly from a php associative array.  Relays on `array_to_rows` DSL function.

```php
function from_array(array $data);
```

Additional options:

* `withSchema(Schema $schema)` - the schema of the dataset, when not set, it will be auto-detected