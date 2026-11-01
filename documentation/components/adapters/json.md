---
package: flow-php/etl-adapter-json
---

# ETL Adapter: JSON

[PACKAGE_NAV]

[TOC]

The JSON adapter reads and writes JSON data in ETL (Extract, Transform, Load) pipelines.

## Installation

For the installation instructions, read
the [installation page](/documentation/installation/packages/etl-adapter-json.md).

## Extractor

```php
<?php

use function Flow\ETL\Adapter\JSON\{from_json, from_json_lines};
use function Flow\ETL\DSL\{data_frame, to_output};

data_frame()
    ->read(from_json(__DIR__ . '/data.json'))
    ->write(to_output())
    ->run();

data_frame()
    ->read(from_json_lines(__DIR__ . '/data.jsonl'))
    ->write(to_output())
    ->run();
```

Both return a `JsonExtractor`. `from_json()` reads one document: a top-level array, or the subtree selected with
`withPointer()`.

In JSON lines, a line with only whitespace (space, tab, CR, LF, VT, FF) is skipped. You cannot configure this.

When the [`flow_php`](/documentation/components/extensions/flow-php-ext.md) extension is loaded, it reads JSON lines
and a document with a top-level array natively, as strict JSON. Malformed input throws
`RuntimeException('Malformed JSON in "<uri>" at line N: ...')` (`at element N` in a document). The PHP reader reads a
pointer, or a document that is not an array.

A value that does not cast to the type that `->withSchema()` declares throws `CorruptedRecordException`, or goes to
`->withCorruptedRecords($loader)`. For more information, read
[Corrupted Records](/documentation/components/core/corrupted-records.md). Under an inferred schema, a key that first
shows after the inference sample throws `InferredSchemaException`.

## Loader

```php
<?php

use function Flow\ETL\Adapter\JSON\{to_json, to_json_lines};
use function Flow\ETL\DSL\{data_frame, from_array};

data_frame()
    ->read(from_array([['id' => 1, 'name' => 'name_1'], ['id' => 2, 'name' => 'name_2']]))
    ->write(to_json(\sys_get_temp_dir() . '/file.json'))
    ->write(to_json_lines(\sys_get_temp_dir() . '/file.jsonl'))
    ->run();
```

Both return a `JsonLoader`. `to_json()` writes one array document, `to_json_lines()` writes
[JSON lines](https://jsonlines.org/).

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

The shape follows the column type, never the data:

- A `list` is always `[...]`.
- A `map`, a `structure` and a row are always `{...}`. An empty map is `{}`, and a map with the keys `0, 1` is
  `{"0":...,"1":...}`.
- A `json` column keeps its objects and lists.
- A `datetime` / `date` is written with `withDateTimeFormat()` / `withDateFormat()` at each depth, and a `time` as
  microseconds.
- A float has the shortest digits that read back as the same float. `1.0` is `1`, or `1.0` under
  `JSON_PRESERVE_ZERO_FRACTION`.
- `NAN` and the infinities are refused:
  `RuntimeException('Failed to encode JSON: Inf and NaN cannot be JSON encoded')`.

Floats are rendered under `serialize_precision = -1`. If the ini has a different value, the writer sets it for the
render and then restores it. `ini_set()` must therefore be allowed. If it is not allowed, the writer throws
`RuntimeException('Writing floats requires serialize_precision = -1 and ini_set() cannot change it')`.

The [`flow_php`](/documentation/components/extensions/flow-php-ext.md) extension renders the same bytes natively.
This works only with the flags `JSON_THROW_ON_ERROR`, `JSON_UNESCAPED_SLASHES`, `JSON_UNESCAPED_UNICODE` and
`JSON_PRESERVE_ZERO_FRACTION`, or a subset of them.

## JSON Schema conversion

The adapter converts [JSON Schema](https://json-schema.org) documents into Flow schemas and back.

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
| heterogeneous `type` arrays, `anyOf`, `oneOf`                                | refused: `UnsupportedUnionTypeException`, at any depth            |
| `allOf`                                                                      | deep merged object schema                                         |

The annotations of top level properties are preserved in definition metadata under `json_schema.*` keys.
`schema_to_json_schema()` writes them again. Annotations are `description`, `title`, `default`, `examples` and
constraints such as `pattern` or `minimum`.

### References

`$ref` is resolved automatically:

- internal pointers (`#/$defs/...`, legacy `#/definitions/...`),
- relative file paths (resolved against the location of the schema file). The schema must load from a `Path`.
- remote `http(s)://` documents. Give a PSR-18 client and a PSR-17 request factory to `schema_from_json_schema()`.

### Limitations

- A Flow schema cannot represent recursive schemas. Circular references throw `CircularReferenceException`.
- `$anchor`, `not`, `if`/`then`/`else`, `contains`, `patternProperties`, `unevaluatedProperties`, `unevaluatedItems`,
  `dependentSchemas`, `dependentRequired`, `$dynamicRef`, `$dynamicAnchor` and `propertyNames` are not supported and
  throw `UnsupportedKeywordException`.
- The conversion inlines references. `schema_to_json_schema()` does not make `$ref` structures again.
- Annotations of nested (non top level) properties are not preserved.
- Accept-anything properties are written as the boolean schema `true`, because an empty PHP array serializes to a
  JSON list. The boolean schema `false` (reject everything) cannot be represented and throws.
- An empty Flow schema cannot be converted to JSON Schema and throws.
