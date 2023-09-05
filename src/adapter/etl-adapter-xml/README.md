# ETL Adapter: XML

# Contributing

This repo is **READ ONLY**, in order to contribute to Flow PHP project, please
open PR against [flow](https://github.com/flow-php/flow) monorepo.

Changes merged to monorepo are automatically propagated into sub repositories.

## Description

ETL Adapter that provides memory safe XML support for ETL.

## Installation

```
composer require flow-php/etl-adapter-xml:1.x@dev
```

## Extractor - XMLExtractor

Memory safe XML extractor 

`xml/simple_items.xml`

```xml
<root>
    <items>
        <item><id>1</id></item>
        <item><id>2</id></item>
        <item><id>3</id></item>
        <item><id>4</id></item>
        <item><id>5</id></item>
        <item><id>6</id></item>
    </items>
</root>
```

```php 
<?php

(new Flow())
    ->read(XML::from(__FLOW_DATA__ . '/simple_items.xml', 'root/items/item'))
    ->write(To::output(false))
    ->run()
;
```

Above code will generate Rows with 5 entries like the one below:

```shell
+----------------------------------------------+
|                                          row |
+----------------------------------------------+
| <?xml version="1.0"?><item><id>1</id></item> |
| <?xml version="1.0"?><item><id>2</id></item> |
| <?xml version="1.0"?><item><id>3</id></item> |
| <?xml version="1.0"?><item><id>4</id></item> |
| <?xml version="1.0"?><item><id>5</id></item> |
| <?xml version="1.0"?><item><id>6</id></item> |
+----------------------------------------------+
```

Each entry will be an XMLEntry type. 
From there you can use built in expressions to extract data from XML.

- `ref('row')->xpath('...');`
- `ref('row')->domNodeAttribute('...');`
- `ref('row')->domNodeValue('...');`

When working with collections XPath will return an ListEntry with XMLEntries inside. 
From there you can for example unpack or expand them. 

For more examples please look into `/examples/topics/xml` directory in [flow monorepo](https://github.com/flow-php/flow)

