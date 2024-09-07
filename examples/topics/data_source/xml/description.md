Read data from a json file.

```php
function from_xml(string|Path $path);
```

Additional options:

* `withXMLNodePath(string $xmlNodePath)` - XML Node Path doesn’t support attributes, and it's not xpath, it is just a sequence of node names separated with slash
* `withBufferSize(int $size)` - default 8096, the size of the buffer used to iterate through stream