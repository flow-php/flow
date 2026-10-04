# Upgrade Guide 0.16.x - 0.35.x

[TOC]

This document provides guidelines for upgrading between versions of Flow PHP. Please follow the instructions for your
specific version to ensure a smooth upgrade process.

Upgrading from older versions: [0.8.x to 0.16.x](/documentation/upgrading-0.8-0.16.md)

---

## Upgrading from 0.34.x to 0.35.x

### 1) `flow-php/postgresql` - `DataType` renamed to `ColumnType`

The `DataType` class used for schema/DDL definitions has been renamed to `ColumnType` to better communicate its purpose.
All related DSL functions have been renamed from `data_type_*` to `column_type_*`.

| Removed                                        | Replacement                                      |
|------------------------------------------------|--------------------------------------------------|
| `Flow\PostgreSql\QueryBuilder\Schema\DataType` | `Flow\PostgreSql\QueryBuilder\Schema\ColumnType` |
| `Flow\PostgreSql\Parser\DataTypeParser`        | `Flow\PostgreSql\Parser\ColumnTypeParser`        |
| `data_type_integer()`                          | `column_type_integer()`                          |
| `data_type_smallint()`                         | `column_type_smallint()`                         |
| `data_type_bigint()`                           | `column_type_bigint()`                           |
| `data_type_boolean()`                          | `column_type_boolean()`                          |
| `data_type_text()`                             | `column_type_text()`                             |
| `data_type_varchar()`                          | `column_type_varchar()`                          |
| `data_type_char()`                             | `column_type_char()`                             |
| `data_type_numeric()`                          | `column_type_numeric()`                          |
| `data_type_decimal()`                          | `column_type_decimal()`                          |
| `data_type_real()`                             | `column_type_real()`                             |
| `data_type_double_precision()`                 | `column_type_double_precision()`                 |
| `data_type_date()`                             | `column_type_date()`                             |
| `data_type_time()`                             | `column_type_time()`                             |
| `data_type_timestamp()`                        | `column_type_timestamp()`                        |
| `data_type_timestamptz()`                      | `column_type_timestamptz()`                      |
| `data_type_interval()`                         | `column_type_interval()`                         |
| `data_type_uuid()`                             | `column_type_uuid()`                             |
| `data_type_json()`                             | `column_type_json()`                             |
| `data_type_jsonb()`                            | `column_type_jsonb()`                            |
| `data_type_bytea()`                            | `column_type_bytea()`                            |
| `data_type_inet()`                             | `column_type_inet()`                             |
| `data_type_cidr()`                             | `column_type_cidr()`                             |
| `data_type_macaddr()`                          | `column_type_macaddr()`                          |
| `data_type_serial()`                           | `column_type_serial()`                           |
| `data_type_smallserial()`                      | `column_type_smallserial()`                      |
| `data_type_bigserial()`                        | `column_type_bigserial()`                        |
| `data_type_array()`                            | `column_type_array()`                            |
| `data_type_custom()`                           | `column_type_custom()`                           |
| `data_type_from_string()`                      | `column_type_from_string()`                      |

Before:

```php
use Flow\PostgreSql\QueryBuilder\Schema\DataType;
use function Flow\PostgreSql\DSL\data_type_integer;
use function Flow\PostgreSql\DSL\data_type_varchar;

column('age', data_type_integer());
column('name', data_type_varchar(255));
cast(ref('id'), DataType::bigint());
```

After:

```php
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_varchar;

column('age', column_type_integer());
column('name', column_type_varchar(255));
cast(ref('id'), ColumnType::bigint());
```

### 2) `flow-php/postgresql` - `PostgreSqlType` renamed to `ValueType`

The `PostgreSqlType` enum used for value binding/casting has been renamed to `ValueType` to better communicate its
purpose. All related DSL functions have been renamed from `pgsql_type_*` to `value_type_*`.

| Removed                                       | Replacement                              |
|-----------------------------------------------|------------------------------------------|
| `Flow\PostgreSql\Client\Types\PostgreSqlType` | `Flow\PostgreSql\Client\Types\ValueType` |
| `pgsql_type_text()`                           | `value_type_text()`                      |
| `pgsql_type_varchar()`                        | `value_type_varchar()`                   |
| `pgsql_type_integer()`                        | `value_type_integer()`                   |
| `pgsql_type_bigint()`                         | `value_type_bigint()`                    |
| `pgsql_type_smallint()`                       | `value_type_smallint()`                  |
| `pgsql_type_boolean()`                        | `value_type_boolean()`                   |
| `pgsql_type_float4()`                         | `value_type_float4()`                    |
| `pgsql_type_float8()`                         | `value_type_float8()`                    |
| `pgsql_type_numeric()`                        | `value_type_numeric()`                   |
| `pgsql_type_date()`                           | `value_type_date()`                      |
| `pgsql_type_timestamp()`                      | `value_type_timestamp()`                 |
| `pgsql_type_timestamptz()`                    | `value_type_timestamptz()`               |
| `pgsql_type_json()`                           | `value_type_json()`                      |
| `pgsql_type_jsonb()`                          | `value_type_jsonb()`                     |
| `pgsql_type_uuid()`                           | `value_type_uuid()`                      |
| `pgsql_type_bytea()`                          | `value_type_bytea()`                     |
| `pgsql_type_inet()`                           | `value_type_inet()`                      |
| `pgsql_type_cidr()`                           | `value_type_cidr()`                      |
| All other `pgsql_type_*()` functions          | Corresponding `value_type_*()` functions |

Before:

```php
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use function Flow\PostgreSql\DSL\pgsql_type_uuid;
use function Flow\PostgreSql\DSL\pgsql_type_text_array;

typed('550e8400-e29b-41d4-a716-446655440000', pgsql_type_uuid());
typed(['tag1', 'tag2'], pgsql_type_text_array());
typed(42, PostgreSqlType::INT4);
```

After:

```php
use Flow\PostgreSql\Client\Types\ValueType;
use function Flow\PostgreSql\DSL\value_type_uuid;
use function Flow\PostgreSql\DSL\value_type_text_array;

typed('550e8400-e29b-41d4-a716-446655440000', value_type_uuid());
typed(['tag1', 'tag2'], value_type_text_array());
typed(42, ValueType::INT4);
```

---

## Upgrading from 0.31.x to 0.32.x

### 1) Removal of Meilisearch Adapter

The Meilisearch adapter has been removed from Flow PHP. If you were using it, please migrate to Elasticsearch adapter.

### 2) Removed deprecated DSL functions

All type-related DSL functions have been moved from `Flow\ETL\DSL` to `Flow\Types\DSL`. Update your imports accordingly.

| Removed Function          | Replacement                               |
|---------------------------|-------------------------------------------|
| `chunks_from()`           | `batches()`                               |
| `type_structure()`        | `\Flow\Types\DSL\type_structure()`        |
| `type_union()`            | `\Flow\Types\DSL\type_union()`            |
| `type_optional()`         | `\Flow\Types\DSL\type_optional()`         |
| `type_from_array()`       | `\Flow\Types\DSL\type_from_array()`       |
| `is_nullable()`           | `\Flow\Types\DSL\type_is_nullable()`      |
| `type_equals()`           | `\Flow\Types\DSL\type_equals()`           |
| `types()`                 | `\Flow\Types\DSL\types()`                 |
| `type_list()`             | `\Flow\Types\DSL\type_list()`             |
| `type_map()`              | `\Flow\Types\DSL\type_map()`              |
| `type_json()`             | `\Flow\Types\DSL\type_json()`             |
| `type_datetime()`         | `\Flow\Types\DSL\type_datetime()`         |
| `type_date()`             | `\Flow\Types\DSL\type_date()`             |
| `type_time()`             | `\Flow\Types\DSL\type_time()`             |
| `type_xml()`              | `\Flow\Types\DSL\type_xml()`              |
| `type_xml_element()`      | `\Flow\Types\DSL\type_xml_element()`      |
| `type_uuid()`             | `\Flow\Types\DSL\type_uuid()`             |
| `type_int()`              | `\Flow\Types\DSL\type_integer()`          |
| `type_integer()`          | `\Flow\Types\DSL\type_integer()`          |
| `type_string()`           | `\Flow\Types\DSL\type_string()`           |
| `type_float()`            | `\Flow\Types\DSL\type_float()`            |
| `type_boolean()`          | `\Flow\Types\DSL\type_boolean()`          |
| `type_instance_of()`      | `\Flow\Types\DSL\type_instance_of()`      |
| `type_resource()`         | `\Flow\Types\DSL\type_resource()`         |
| `type_array()`            | `\Flow\Types\DSL\type_array()`            |
| `type_callable()`         | `\Flow\Types\DSL\type_callable()`         |
| `type_null()`             | `\Flow\Types\DSL\type_null()`             |
| `type_enum()`             | `\Flow\Types\DSL\type_enum()`             |
| `struct_schema()`         | `structure_schema()`                      |
| `get_type()`              | `\Flow\Types\DSL\get_type()`              |
| `print_schema()`          | `schema_to_ascii()`                       |
| `type_is()`               | `\Flow\Types\DSL\type_is()`               |
| `type_is_any()`           | `\Flow\Types\DSL\type_is_any()`           |
| `dom_element_to_string()` | `\Flow\Types\DSL\dom_element_to_string()` |

### 3) Removed deprecated DataFrame methods

| Removed Method                         | Replacement                                                  |
|----------------------------------------|--------------------------------------------------------------|
| `DataFrame::validate()`                | `DataFrame::match()`                                         |
| `DataFrame::renameAll()`               | `DataFrame::renameEach(rename_replace(...))`                 |
| `DataFrame::renameAllLowerCase()`      | `DataFrame::renameEach(rename_style(StringStyles::LOWER))`   |
| `DataFrame::renameAllUpperCase()`      | `DataFrame::renameEach(rename_style(StringStyles::UPPER))`   |
| `DataFrame::renameAllUpperCaseFirst()` | `DataFrame::renameEach(rename_style(StringStyles::UCFIRST))` |
| `DataFrame::renameAllUpperCaseWord()`  | `DataFrame::renameEach(rename_style(StringStyles::UCWORDS))` |
| `DataFrame::renameAllStyle()`          | `DataFrame::renameEach(rename_style(...))`                   |

### 4) Removed deprecated Schema methods

| Removed Method            | Replacement                   |
|---------------------------|-------------------------------|
| `Schema::entries()`       | `Schema::references()->all()` |
| `Schema::getDefinition()` | `Schema::get()`               |
| `Schema::nullable()`      | `Schema::makeNullable()`      |

### 5) Removed deprecated Definition methods

| Removed Method           | Replacement                  |
|--------------------------|------------------------------|
| `Definition::nullable()` | `Definition::makeNullable()` |

This applies to all Definition implementations: `BooleanDefinition`, `DateDefinition`, `DateTimeDefinition`,
`EnumDefinition`, `FloatDefinition`, `HTMLDefinition`, `HTMLElementDefinition`, `IntegerDefinition`, `JsonDefinition`,
`ListDefinition`, `MapDefinition`, `StringDefinition`, `StructureDefinition`, `TimeDefinition`, `UuidDefinition`,
`XMLDefinition`, `XMLElementDefinition`.

### 6) Removed deprecated FileExtractor and PathFiltering methods

| Removed Method               | Replacement                       |
|------------------------------|-----------------------------------|
| `FileExtractor::addFilter()` | `FileExtractor::withPathFilter()` |
| `PathFiltering::addFilter()` | `PathFiltering::withPathFilter()` |

`withPathFilter()` is removed in 0.45 - see 8) of that version.

### 7) Removed deprecated ScalarFunctionChain methods

| Removed Method                               | Replacement                                       |
|----------------------------------------------|---------------------------------------------------|
| `ScalarFunctionChain::domElementAttribute()` | `ScalarFunctionChain::domElementAttributeValue()` |

### 8) Removed deprecated Config constants

| Removed Constant              | Replacement                       |
|-------------------------------|-----------------------------------|
| `Config::CACHE_DIR_ENV`       | `CacheConfig::CACHE_DIR_ENV`      |
| `Config::SORT_MAX_MEMORY_ENV` | `SortConfig::SORT_MAX_MEMORY_ENV` |

### 9) Removed deprecated Transformers

| Removed Transformer                     | Replacement                                      |
|-----------------------------------------|--------------------------------------------------|
| `EntryNameStyleConverterTransformer`    | Use `DataFrame::renameEach(rename_style(...))`   |
| `RenameAllCaseTransformer`              | Use `DataFrame::renameEach(rename_style(...))`   |
| `RenameStrReplaceAllEntriesTransformer` | Use `DataFrame::renameEach(rename_replace(...))` |

### 10) Removed deprecated classes

| Removed Class                                   | Replacement                    |
|-------------------------------------------------|--------------------------------|
| `Flow\ETL\Function\StyleConverter\StringStyles` | `Flow\ETL\String\StringStyles` |

---

## Upgrading from 0.28.x to 0.29.x

### 1) JsonType now uses Json value object instead of string

The `JsonType` has been refactored to use a dedicated `Json` value object (similar to `Uuid`/`UuidType` pattern). This
allows static analysis tools to distinguish between regular strings and JSON strings.

**Breaking Changes:**

- `JsonType::assert()` now returns `Json` instance instead of `string`
- `JsonType::cast()` now returns `Json` instance instead of `string`
- `JsonType::isValid()` now checks for `Json` instance (plain strings are no longer valid)
- `Cast::cast('json', $value)` function now returns `Json` object instead of string
- `type_json()` return type annotation changed from `Type<string>` to `Type<Json>`
- `JsonEntry::value()` now returns `?Json` instead of `?array` (consistent with `UuidEntry::value()` returning `?Uuid`)
- `JsonEntry::json()` method removed (use `value()` instead)

**Migration:**

If you were using `type_json()->cast($value)` and expected a string, use `->toString()`:

Before:

```php
$jsonString = type_json()->cast($array); // was string
```

After:

```php
$json = type_json()->cast($array); // now Json object
$jsonString = $json->toString(); // get the string
$jsonArray = $json->toArray(); // get as array
```

If you were using `JsonEntry::value()` and expected an array:

Before:

```php
$entry = json_entry('data', ['key' => 'value']);
$array = $entry->value(); // was array
```

After:

```php
$entry = json_entry('data', ['key' => 'value']);
$json = $entry->value(); // now Json object
$array = $json?->toArray(); // get as array
$string = $json?->toString(); // get as string
```

If you were using `JsonEntry::json()`:

Before:

```php
$json = $entry->json();
```

After:

```php
$json = $entry->value(); // json() method removed, use value() instead
```

**New Json value object features:**

```php
use Flow\Types\Value\Json;

// Create from string
$json = new Json('{"key": "value"}');

// Create from array
$json = Json::fromArray(['key' => 'value']);

// Check if valid JSON
Json::isValid('{"key": "value"}'); // true

// Convert to string/array
$json->toString(); // '{"key":"value"}'
$json->toArray(); // ['key' => 'value']

// Json implements Stringable
(string) $json; // '{"key":"value"}'

// Json implements JsonSerializable
json_encode($json); // '{"key":"value"}'
```

**Note:** `JsonEntry::value()` now returns `?Json` for consistency with `UuidEntry::value()` returning `?Uuid`. Use
`->toArray()` or `->toString()` on the Json object to get the underlying data.

**Row methods behavior:**

```php
// Row::toArray() converts Json to array automatically (for convenient serialization)
$row->toArray();           // Returns ['data' => ['key' => 'value']] not ['data' => Json(...)]

// Row::valueOf() returns the raw value (Json object for json entries)
$row->valueOf('data');     // Returns Json object (use ->toArray() if you need array)

// Entry value() returns the typed value
$row->get('data')->value();  // Returns Json object (use ->toArray() if you need array)
```

---

## Upgrading from 0.26.x to 0.27.x

### 1) Force `EntryFactory $entryFactory` to be required on `array_to_row` & `array_to_row(s)`

Before:

```php
to_entry('name', 'data');
array_to_row([]);
array_to_rows([]);
```

After:

```php
to_entry('name', 'data', flow_context(config())->entryFactory());
array_to_row([], flow_context(config())->entryFactory());
array_to_rows([], flow_context(config())->entryFactory());
```

---

## Upgrading from 0.16.x to 0.17.x

### 1) Removed $nullable property from all types

Before:

```php
type_string(nullable:true)->toString() // ?string
```

After:

```php
type_optional(string())->toString() // ?string
```

### 2) Removed precision from `float_type()`

Before `float_type()` use to have default precision 6. This means that any operations on float had to round values to
given precision. The problem with this approach is that all operations now need to receive a dedicated rounding option.

Instead, end users should handle precision of float columns through `round()` scalar function.

### 3) Moved all Types to `Flow\Types\Type` namespace

Before

```php
\Flow\ETL\DSL\type_string(); // now deprecated, alias for \Flow\Types\DSL\type_string();
```

After

```php
\Flow\Types\DSL\type_string();
```
