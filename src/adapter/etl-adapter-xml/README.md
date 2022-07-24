# ETL Adapter: XML

# Contributing

This repo is **READ ONLY**, in order to contribute to Flow PHP project, please
open PR against [flow](https://github.com/flow-php/flow) monorepo.

Changes merged to monorepo are automatically propagated into sub repositories.

## Description

ETL Adapter that provides memory safe XML support for ETL.

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
    ->read(XML::from_file(__DIR__ . '/xml/simple_items.xml', 'root/items/item'))
    ->fetch()
```

Above code will generate Rows with 5 entries like the one below:

```php
<?php

Row::create(
    Entry::array('row', [
        'item' => [
            'id' => [
                '@value' => 1
            ]
        ]
    ])
)
```