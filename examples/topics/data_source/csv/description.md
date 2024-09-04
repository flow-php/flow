Read data from a csv file.

```php
function from_csv(
    string|Path $path,
    bool $with_header = true,
    bool $empty_to_null = true,
    ?string $delimiter = null,
    ?string $enclosure = null,
    ?string $escape = null,
    int $characters_read_in_line = 1000,
    ?Schema $schema = null
):
``` 

* `with_header` - default true, if false, the first row will be treated as data
* `empty_to_null` - default false, if true, empty string will be treated as null
* `delimiter` - the delimiter of the csv file, when not set, it will be auto detected
* `enclosure` - the enclosure of the csv file, when not set, it will be auto detected
* `escape` - default `\`, the escape character of the csv file
* `characters_in_line` - default `1000`, size of chunk used to read lines from the file
* `schema` - the schema of the csv file, when not set, it will be auto detected