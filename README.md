# ETL Adapter: XML

[![Minimum PHP Version](https://img.shields.io/badge/php-~8.1-8892BF.svg)](https://php.net/)

## Description

ETL Adapter that provides memory safe XML support for ETL.

## Entry - XMLEntry

```php 
<?php

use Flow\ETL\Row\Entry\XMLEntry;

$entry = XMLEntry::fromString('xml_entry', '<xml><name>node</name></xml>')
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

$extractor = new XMLReaderExtractor(
    $xmlFile = __DIR__ . '/xml/simple_items.xml', 
    $xmlNodePath = 'root/items/item', 
    $rowsInBatch = 5, 
    $rowEntryName = 'row'
);
$rowsGenerator = $extractor->extract();

$this->assertSame(5, $rowsGenerator->current()->count());
$this->assertInstanceOf(\DOMDocument::class, $xml = $rowsGenerator->current()->first()->valueOf('row'));
$this->assertXmlStringEqualsXmlString('<item><id>1</id></item>', $xml->saveHTML());

$rowsGenerator->next();

$this->assertSame(1, $rowsGenerator->current()->count());
$this->assertInstanceOf(\DOMDocument::class, $xml = $rowsGenerator->current()->first()->valueOf('row'));
$this->assertXmlStringEqualsXmlString('<item><id>6</id></item>', $xml->saveHTML());
```
