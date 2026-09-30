---
package: flow-php/etl-adapter-xml
---

# ETL Adapter: XML

[PACKAGE_NAV]

[TOC]

Flow PHP's Adapter XML is a dedicated library engineered to facilitate seamless interactions with XML data within your
ETL (Extract, Transform, Load) processes. This adapter empowers developers to effortlessly extract from and load data
into XML formats, ensuring a smooth and reliable data transformation journey. By harnessing the Adapter XML library,
developers can tap into a robust set of features designed for precise XML data handling, making complex data
transformations both manageable and efficient. The Adapter XML library encapsulates a rich set of functionalities,
providing a streamlined API for interacting with XML data, which is indispensable in modern data processing and
transformation workflows. This library embodies Flow PHP's commitment to providing versatile data processing solutions,
making it a prime choice for developers dealing with XML data in large-scale and data-intensive environments. With Flow
PHP's Adapter XML, managing XML data within your ETL workflows becomes a more simplified and efficient endeavor,
aligning perfectly with the robust and adaptable nature of the Flow PHP ecosystem.

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/etl-adapter-xml.md).

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

| Column                     | Element                                                                                                                            |
|----------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| `boolean`                  | `true` / `false`                                                                                                                   |
| `float`                    | the shortest text that reads back as the same float, always with a fraction or an exponent: `1.0`, `1.0e+25`; `NAN`, `INF`, `-INF` |
| `null`                     | an empty element                                                                                                                   |
| `datetime`, `date`         | `withDateTimeFormat()` (default `Y-m-d\TH:i:s.uP`) in the column zone, `withDateFormat()` (default `Y-m-d`)                        |
| `time`                     | microseconds                                                                                                                       |
| `list`, `map`, `structure` | nested elements; every leaf is written as a column of its type would be                                                            |
| `json`                     | the stored text                                                                                                                    |

A column whose name starts with the attribute prefix (`_` by default) is an attribute of its row; a null there is
refused.

Floats are rendered under `serialize_precision = -1`. When the ini holds another value the writer sets it for the
render and restores it, so `ini_set()` must be allowed; otherwise it throws
`RuntimeException('Writing floats requires serialize_precision = -1 and ini_set() cannot change it')`.

### Custom writers

`XMLWriter` renders a column at a time: `elements()` and `attributes()` return one string per value, with the writer's
own escaping. A writer that only builds nodes delegates to `write()`:

```php
<?php

use Flow\ETL\Adapter\XML\Abstraction\{XMLAttribute, XMLNode};
use Flow\ETL\Adapter\XML\XMLWriter;

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