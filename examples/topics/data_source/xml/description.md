Read data from a json file.

```php
function from_xml(
    string|Path $path,
    string $xml_node_path = ''
);
```

* `xml_node_path` - default '', the path to the node to read, when empty, the root node will be read. It's not xpath, it is just a sequence of node names separated with slash.