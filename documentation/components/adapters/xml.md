---
package: flow-php/etl-adapter-xml
---

# ETL Adapter: XML

[PACKAGE_NAV]

[TOC]

The XML adapter reads and writes XML data in ETL (Extract, Transform, Load) pipelines.

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/etl-adapter-xml.md).

## Extractor

`simple_items.xml`

```xml
<root>
    <items>
        <item><id>1</id></item>
        <item><id>2</id></item>
        <item><id>3</id></item>
    </items>
</root>
```

```php
<?php

use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\{data_frame, ref, to_output};

data_frame()
    ->read(from_xml(__DIR__ . '/simple_items.xml')->withXMLNodePath('root/items/item'))
    ->withColumn('id', ref('node')->xpath('id')->domElementValue())
    ->write(to_output(truncate: false))
    ->run();
```

```shell
+-------------------------+----+
|                    node | id |
+-------------------------+----+
| <item><id>1</id></item> |  1 |
| <item><id>2</id></item> |  2 |
| <item><id>3</id></item> |  3 |
+-------------------------+----+
3 rows
```

The file is streamed. Each node at `withXMLNodePath()` is one row with one `xml` column `node`. Read it with
`xpath()` (returns a `list<xml_element>`), `domElementValue()`, `domElementAttributeValue()` and the other
`domElement*()` functions.

## Loader

```php
<?php

use function Flow\ETL\Adapter\XML\to_xml;
use function Flow\ETL\DSL\{bool_schema, data_frame, datetime_schema, float_schema, from_array, int_schema, json_schema, list_schema, map_schema, schema, structure_schema};
use function Flow\Types\DSL\{type_date, type_integer, type_list, type_map, type_string, type_structure};

data_frame()
    ->read(from_array(
        [
            ['id' => 1, 'active' => true, 'price' => 0.1 + 0.2, 'at' => new DateTimeImmutable('2026-01-02 03:04:05 UTC'), 'days' => [new DateTimeImmutable('2026-01-02')], 'labels' => [0 => 'a', 1 => 'b'], 'size' => ['w' => 1, 'h' => 2], 'meta' => '{"tags": []}'],
            ['id' => 2, 'active' => false, 'price' => 1.0, 'at' => null, 'days' => [], 'labels' => [], 'size' => ['w' => 3, 'h' => 4], 'meta' => '{}'],
        ],
        schema(
            int_schema('id'), bool_schema('active'), float_schema('price'), datetime_schema('at', nullable: true),
            list_schema('days', type_list(type_date())), map_schema('labels', type_map(type_integer(), type_string())),
            structure_schema('size', type_structure(['w' => type_integer(), 'h' => type_integer()])), json_schema('meta'),
        ),
    ))
    ->write(to_xml($path))
    ->run();
```

```xml
<?xml version="1.0" encoding="UTF-8"?>
<rows>
<row><id>1</id><active>true</active><price>0.30000000000000004</price><at>2026-01-02T03:04:05.000000+00:00</at><days><element>2026-01-02</element></days><labels><element><key>0</key><value>a</value></element><element><key>1</key><value>b</value></element></labels><size><w>1</w><h>2</h></size><meta>{"tags": []}</meta></row>
<row><id>2</id><active>false</active><price>1.0</price><at></at><days/><labels/><size><w>3</w><h>4</h></size><meta>{}</meta></row>
</rows>
```

| Column                     | Element                                                                                                                                 |
|----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `boolean`                  | `true` / `false`                                                                                                                        |
| `float`                    | the shortest text that reads back as the same float, always with a fraction or an exponent: `1.0`, `1.0e+25`. Also `NAN`, `INF`, `-INF` |
| `null`                     | an empty element                                                                                                                        |
| `datetime`, `date`         | `withDateTimeFormat()` (default `Y-m-d\TH:i:s.uP`) in the column zone, `withDateFormat()` (default `Y-m-d`)                             |
| `time`                     | microseconds                                                                                                                            |
| `list`, `map`, `structure` | nested elements. Each leaf is written like a column of its type                                                                         |
| `json`                     | the stored text                                                                                                                         |

A column with a name that starts with the attribute prefix (`_` by default) is an attribute of its row. A null in
such a column is refused.

Floats are rendered under `serialize_precision = -1`. If the ini has a different value, the writer sets it for the
render and then restores it. `ini_set()` must therefore be allowed. If it is not allowed, the writer throws
`RuntimeException('Writing floats requires serialize_precision = -1 and ini_set() cannot change it')`.

### Custom writers

`XMLWriter` renders one column at a time. `elements()` and `attributes()` return one string for each value, with the
escaping of the writer. A writer that only builds nodes delegates to `write()`:

```php
<?php

use Flow\ETL\Adapter\XML\Abstraction\{XMLAttribute, XMLNode};
use Flow\ETL\Adapter\XML\IO\Loader\XMLWriter;

use function Flow\Types\DSL\type_string;

final class MyWriter implements XMLWriter
{
    public function attributes(string $name, array $values): array
    {
        return array_map(
            fn (?string $value): string => substr($this->write(XMLNode::nested('x', new XMLAttribute($name, type_string()->cast($value)))), 2, -2),
            $values,
        );
    }

    public function elements(string $name, array $values): array
    {
        return array_map(fn (?string $value): string => $this->write(XMLNode::flatNode($name, $value ?? '')), $values);
    }

    public function write(XMLNode $node): string
    {
        // ...
    }
}
```