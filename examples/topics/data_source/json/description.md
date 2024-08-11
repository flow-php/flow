Read data from a json file.

```php
function from_json(
    string|Path|array $path,
    ?string $pointer = null,
    ?Schema $schema = null,
);
```

* `pointer` - default null, used to iterate only results of a subtree, read more about [pointers](https://github.com/halaxa/json-machine#parsing-a-subtree)
* `schema` - the schema of the csv file, when not set, it will be auto detected