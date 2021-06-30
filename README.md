# Flow PHP 

Flow PHP is a DSL for [Flow PHP ETL](https://github.com/flow-php/etl) and all core elements. 
This library is a full bundle of all ETL components with neat functional interface.

```
composer require flow-php/flow
```

## Example

```php
<?php

use function Flow\ETL\DSL\Transformer\{convert_name, filter_equals, keep, to_datetime, to_json};
use function Flow\ETL\DSL\Extractor\{extract_from_array};
use function Flow\ETL\DSL\Loader\{to_csv};

$data = [
    [
        'id' => 1,
        'name' => 'Norbert',
        'status' => 'premium',
        'updatedAt' => '2020-01-01 00:00:00',
        'properties' => [1, 2, 3]
    ],
    [
        'id' => 2,
        'name' => 'John',
        'status' => 'premium',
        'updatedAt' => '2020-01-02 00:00:00',
        'properties' => [4, 5]
    ],
    [
        'id' => 3,
        'name' => 'Steve',
        'status' => 'free',
        'updatedAt' => '2020-01-03 00:00:00',
        'properties' => [6]
    ],
];

extract_from_array($data)
    ->transform(filter_equals('status', 'premium'))
    ->transform(convert_name('snake'))
    ->transform(keep('id', 'name', 'updated_at', 'properties'))
    ->transform(to_datetime(['updated_at'], 'Y-m-d H:i:s', 'UTC'))
    ->transform(to_json('properties'))
    ->load(to_csv(__DIR__ . '/premium_users.csv'))
    ->run();
```

## Domain-specific Language

Each element of the DSL is a simple php function that can be combined together with other functions. 

### Columns

* `string_column(string $name, string $value)`
* `integer_column(string $name, int $value)`
* `boolean_column(string $name, bool $value)`
* `float_column(string $name, float $value)`
* `date_column(string $name, string $value)`
* `datetime_column(string $name, string $value, string $format = \DateTimeImmutable::ATOM)`
* `array_column(string $name, array $data)`
* `json_column(string $name, array $data)`
* `json_object_column(string $name, array $data)`
* `object_column(string $name, object $object)`

### Conditions

* `all(RowCondition ...$conditions)`
* `any(RowCondition ...$conditions)`
* `array_exists(string $column, string $path)`
* `array_value_equals(string $column, string $path, $value, bool $identical = true)`
* `array_value_greaterOrEqual(string $column, string $path, $value)`
* `array_value_greater(string $column, string $path, $value)`
* `array_value_less_or_equal(string $column, string $path, $value)`
* `array_valueLess(string $column, string $path, $value)`
* `exists(string $column)`
* `is_string(string $column)`
* `is_integer(string $column)`
* `is_boolean(string $column)`
* `is_float(string $column)`
* `is_array(string $column)`
* `is_json(string $column)`
* `is_object(string $column)`
* `is_null(string $column)`
* `is_not_null(string $column)`
* `value_equals(string $column, $value, bool $identical = true)`
* `value_greater_or_equal(string $column, $value)`
* `value_greater(string $column, $value)`
* `value_less_or_equal(string $column, $value)`
* `value_less(string $column, $value)`
* `none(RowCondition $conditions)`
* `opposite(RowCondition $condition)`


### Extractors 

* `extract_from_csv(string $file_name, int $batch_size = 100, int $header_offset = 0)`
* `extract_from_array(array $array, int $batch_size = 100)`
* `extract_from_json(string $file_name, int $batch_size = 100)`
* `extract_from_http(ClientInterface $client, iterable $requests, ?callable $pre_request = null, ?callable $post_request = null)`
* `extract_from_http_dynamic(ClientInterface $client, NextRequestFactory $request_factory, ?callable $pre_request = null, ?callable $post_request = null)`
* `extract_from_db(Connection $connection, string $query, ParametersSet $parameters_set = null, array $types = [])`

### Factories

* `column_from_value(string $column, $value)`
* `rows_from_array(array $data)`
* `rows_from_casted_array(array $data, CastEntry ...$cast_entries)`
 
### Loaders

* `to_csv(string $file_name)`
* `to_elastic_search(Client $client, int $chunk_size, string $index, IdFactory $id_factory, array $parameters = [])`
    * `es_id_sha1(string ...$columns)`
    * `es_id_columns(string $column)`
* `to_memory(Memory $memory)`
* `to_debug_logger()`
* `to_column_dumper(bool $all = false)`
  
### Transformers

* `add_string(string $name, string $value)`
* `add_integer(string $name, int $value)`
* `add_boolean(string $name, bool $value)`
* `add_float(string $name, float $value)`
* `add_date(string $name, string $value)`
* `add_datetime(string $name, string $value, string $format = \DateTimeImmutable::ATOM)`
* `add_array(string $name, array $data)`
* `add_json(string $name, array $data)`
* `add_json_object(string $name, array $data)`
* `add_object(string $name, object $data)`
* `array_get(string $array_name, string $path, string $column_name = 'column')`
* `array_sort(string $array_name, $sort_flag = \SORT_REGULAR)`
* `array_reverse(string $array_name)`
* `array_merge(array $array_names, string $column_name = 'column')`
* `clone_column(string $from, string $to)`
* `concat(array $stringColumns, string $glue = '', string $column_name = 'column')`
* `chain(Transformer ...$transformers)`  
* `convert_name(string $style)`  
* `expand(string $array_column, string $expanded_name = 'column')`
* `filter(string $column, callable $filter)`
* `filter_equals(string $column, $value)`
* `filter_not_equals(string $column, $value)`
* `filter_exists(string $column)`
* `filter_not_exists(string $column)`
* `filter_null(string $column)`
* `filter_not_null(string $column)`
* `filter_number(string $column)`
* `filter_not_number(string $column)`
* `keep(string ...$columns)`
* `object_method(string $object_name, string $method, string $column_name = 'column', array $parameters = [])`
* `remove(string ...$columns)`
* `rename(string $from, string $to)`
* `to_datetime(array $columns, $format = 'c', ?string $timezone = null, ?string $to_timezone = null)`
* `to_datetime_cast(array $columns, $format = 'c', ?string $timezone = null, ?string $to_timezone = null)`
* `to_date(string ...$columns)`
* `to_date_cast(string ...$column)`
* `to_integer(string ...$columns)`
* `to_integer_cast(string ...$column)`
* `to_string(string ...$columns)`
* `to_string_cast(string ...$column)`
* `to_json(string ...$columns)`
* `to_json_cast(string ...$column)`
* `to_array_from_json(string ...$columns)`
* `to_array_from_json_cast(string ...$column)`
* `to_null_from_null_string(string ...$columns)`
* `to_array_from_object(string $column)`
* `transform_if(RowCondition $condition, Transformer $transformer)`
* `unpack(string $array_column, string $column_prefix = '', array $skip_keys = [])`