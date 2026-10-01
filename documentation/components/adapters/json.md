---
package: flow-php/etl-adapter-json
---

# ETL Adapter: JSON

[PACKAGE_NAV]

[TOC]

Flow PHP's Adapter JSON is a meticulously engineered library aimed at facilitating seamless interactions with JSON data
within your ETL (Extract, Transform, Load) workflows. This adapter is paramount for developers seeking to effortlessly
extract from or load data into JSON formats, ensuring a fluid and reliable data transformation experience. By utilizing
the Adapter JSON library, developers can harness a robust set of features tailored for precise JSON data handling,
making complex data transformations both manageable and efficient. The Adapter JSON library encapsulates a comprehensive
set of functionalities, providing a streamlined API for engaging with JSON data, which is indispensable in modern data
processing and transformation scenarios. This library embodies Flow PHP's commitment to offering versatile and efficient
data processing solutions, making it a prime choice for developers dealing with JSON data in large-scale and
data-intensive environments. With Flow PHP's Adapter JSON, managing JSON data within your ETL workflows becomes a more
simplified and efficient task, perfectly aligning with the robust and adaptable nature of the Flow PHP ecosystem.

## Installation

For detailed installation instructions, see
the [installation page](/documentation/installation/packages/etl-adapter-json.md).


> Json library is not explicitly required, you need to make sure it is available in your composer.json file.
> If you are only using Loader, this dependency is optional.

## Extractor - JSONMachine - JsonExtractor

```php
<?php

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\{data_frame, to_output};

data_frame()
    ->read(from_json(__DIR__ . '/data.json'))
    ->collect()
    ->write(to_output())
    ->run();
```

In JSON lines, a line holding only whitespace (space, tab, CR, LF, VT, FF) is skipped, as DuckDB does; this is not
configurable.

With the [`flow_php`](/documentation/components/extensions/flow-php-ext.md) extension loaded, JSON lines and a document
holding a top-level array are read natively, as strict JSON: malformed input throws
`RuntimeException('Malformed JSON in "<uri>" at line N: …')` (`at element N` in a document). A pointer, or a document
that is not an array, is read by the PHP reader.

## Loader - JsonLoader

```php
<?php

use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\ETL\DSL\{data_frame, from_array};

data_frame()
    ->read(from_array(
        \array_map(
            fn (int $i) : array => ['id' => $i, 'name' => 'name_' . $i],
            \range(0, 10)
        )
    ))
    ->collect()
    ->write(to_json(\sys_get_temp_dir() . '/file.json'))
    ->run();
```

## Loader - JsonLoader - JSON lines

It is also possible to export the rows using the [json lines](https://jsonlines.org/) format

```php
<?php

use function Flow\ETL\Adapter\JSON\to_json_lines;
use function Flow\ETL\DSL\{data_frame, from_array};

data_frame()
    ->read(from_array(
        \array_map(
            fn (int $i) : array => ['id' => $i, 'name' => 'name_' . $i],
            \range(0, 10)
        )
    ))
    ->collect()
    ->write(to_json_lines(\sys_get_temp_dir() . '/file.jsonl'))
    ->run();
```

## What the loaders write

```php
<?php

use function Flow\ETL\Adapter\JSON\to_json_lines;
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
    ->write(to_json_lines($path))
    ->run();
```

```json
{"id":1,"active":true,"price":0.30000000000000004,"at":"2026-01-02T03:04:05+00:00","days":["2026-01-02"],"labels":{"0":"a","1":"b"},"size":{"w":1,"h":2},"meta":{"tags":[]}}
{"id":2,"active":false,"price":1,"at":null,"days":[],"labels":{},"size":{"w":3,"h":4},"meta":{}}
```

The shape follows the column type, never the data: a `list` is always `[…]`, a `map`, a `structure` and a row are
always `{…}` (an empty map is `{}`, a map keyed `0, 1` is `{"0":…,"1":…}`), a `json` column keeps its objects and
lists. A `datetime` / `date` is written with `withDateTimeFormat()` / `withDateFormat()` at every depth, a `time` as
microseconds. A float has the shortest digits that read back as the same float (`1.0` is `1`, or `1.0` under
`JSON_PRESERVE_ZERO_FRACTION`); `NAN` and the infinities are refused:
`RuntimeException('Failed to encode JSON: Inf and NaN cannot be JSON encoded')`.

Floats are rendered under `serialize_precision = -1`. When the ini holds another value the writer sets it for the
render and restores it, so `ini_set()` must be allowed; otherwise it throws
`RuntimeException('Writing floats requires serialize_precision = -1 and ini_set() cannot change it')`.

With the [`flow_php`](/documentation/components/extensions/flow-php-ext.md) extension loaded and no flags beyond
`JSON_THROW_ON_ERROR`, `JSON_UNESCAPED_SLASHES`, `JSON_UNESCAPED_UNICODE` and `JSON_PRESERVE_ZERO_FRACTION`, the same
bytes are rendered natively.

## JSON Schema conversion

The adapter can convert [JSON Schema](https://json-schema.org) documents into Flow schemas and back.

```php
<?php

use function Flow\ETL\Adapter\JSON\{from_json, schema_from_json_schema, schema_to_json_schema};
use function Flow\ETL\DSL\data_frame;
use function Flow\Filesystem\DSL\path;

$schema = schema_from_json_schema(path(__DIR__ . '/schemas/person.json'));

data_frame()
    ->read(from_json(__DIR__ . '/people.json', schema: $schema))
    ->collect()
    ->run();

$jsonSchema = schema_to_json_schema($schema); // draft 2020-12 document as array
```

`schema_from_json_schema()` accepts a decoded document (array), a raw JSON document (string) or a `Path` to a schema
file.

### Type mapping

| JSON Schema                                                                  | Flow                                                              |
|------------------------------------------------------------------------------|-------------------------------------------------------------------|
| `string` (+ `format: date` / `date-time` / `time` / `uuid` / `html` / `xml`) | `string` / `date` / `datetime` / `time` / `uuid` / `html` / `xml` |
| `integer` / `number` / `boolean` / `null`                                    | `integer` / `float` / `boolean` / null column                     |
| `array` + `items`                                                            | `list<items>`                                                     |
| `array` without `items`                                                      | `json`                                                            |
| `object` + `properties`                                                      | `structure` (`required` controls optional members)                |
| `object` + `additionalProperties: <schema>`                                  | `map<string, value>`                                              |
| `object` without `properties` / `additionalProperties`                       | `json`                                                            |
| `type: ["object", "array"]`                                                  | `json`                                                            |
| empty schema `{}` / boolean schema `true`                                    | `json` (accept anything, marked in metadata)                      |
| `enum` / `const`                                                             | scalar type derived from the values, values preserved in metadata |
| `type: [X, "null"]`, `anyOf`/`oneOf` with a null member                      | nullable X                                                        |
| heterogeneous `type` arrays, `anyOf`, `oneOf`                                | refused - `UnsupportedUnionTypeException`, at any depth           |
| `allOf`                                                                      | deep merged object schema                                         |

Annotations (`description`, `title`, `default`, `examples`, constraints like `pattern` or `minimum`) of top level
properties are preserved in definition metadata under `json_schema.*` keys and re-emitted by `schema_to_json_schema()`.

### References

`$ref` is resolved automatically:

- internal pointers (`#/$defs/...`, legacy `#/definitions/...`),
- relative file paths (resolved against the schema file location, requires loading the schema from a `Path`),
- remote `http(s)://` documents - requires passing a PSR-18 client and a PSR-17 request factory to
  `schema_from_json_schema()`.

### Limitations

- Recursive schemas cannot be represented by a Flow schema, circular references throw `CircularReferenceException`.
- `$anchor`, `not`, `if`/`then`/`else`, `contains`, `patternProperties`, `unevaluatedProperties`, `unevaluatedItems`,
  `dependentSchemas`, `dependentRequired`, `$dynamicRef`, `$dynamicAnchor` and `propertyNames` are not supported and
  throw `UnsupportedKeywordException`.
- References are inlined during conversion, `schema_to_json_schema()` does not reconstruct `$ref` structures.
- Annotations of nested (non top level) properties are not preserved.
- Accept-anything properties are emitted as the boolean schema `true` (an empty PHP array would serialize to a JSON
  list); the boolean schema `false` (reject everything) cannot be represented and throws.
- An empty Flow schema cannot be converted to JSON Schema and throws.
