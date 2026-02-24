Read data from a json file.

```php
function from_json(string|Path $path);
```

Additional options: 

* `withPointer(string $pointer)` - default null, used to iterate only results of a subtree, read more about [pointers](https://github.com/halaxa/json-machine#parsing-a-subtree)
* `withSchema(Schema $schema)` - the schema of the dataset, when not set, it will be auto-detected