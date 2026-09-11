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
| `object` + `properties`                                                      | `structure` (`required` controls optional members)                |
| `object` + `additionalProperties: <schema>`                                  | `map<string, value>`                                              |
| `type: ["object", "array"]`                                                  | `json`                                                            |
| empty schema `{}` / boolean schema `true`                                    | `json` (accept anything, marked in metadata)                      |
| `enum` / `const`                                                             | scalar type derived from the values, values preserved in metadata |
| `type: [X, "null"]`, `anyOf`/`oneOf` with a null member                      | nullable X                                                        |
| heterogeneous `type` arrays, `anyOf`, `oneOf`                                | union type                                                        |
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
