# Flow PHP 

Flow PHP is a DSL for [Flow PHP ETL](https://github.com/flow-php/etl) and all core elements. 
This library is a full bundle of all ETL components with neat functional interface.



```
composer require flow-php/flow
```

## Example

```php
<?php

use function Flow\ETL\DSL\Transformer\{convertName,filter, keep, toDateTime, toJson};
use function Flow\ETL\DSL\Extractor\{extractFromArray};
use function Flow\ETL\DSL\Loader\{toCSV};

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

extractFromArray($data)
    ->transform(filter('status', fn(string $status) => $status === 'premium'))
    ->transform(convertName('snake'))
    ->transform(keep('id', 'name', 'updated_at', 'properties'))
    ->transform(toDateTime(['updated_at'], 'Y-m-d H:i:s', 'UTC'))
    ->transform(toJson('properties'))
    ->load(toCSV(__DIR__ . '/premium_users.csv'))
    ->run();
```

## Domain-specific Language
             
### Columns

* `function stringColumn(string $name, string $value)`
* `function integerColumn(string $name, int $value)`
* `function booleanColumn(string $name, bool $value)`
* `function floatColumn(string $name, float $value)`
* `function dateColumn(string $name, string $value)`
* `function dateTimeColumn(string $name, string $value, string $format = \DateTimeImmutable::ATOM)`
* `function arrayColumn(string $name, array $data)`
* `function jsonColumn(string $name, array $data)`
* `function jsonObjectColumn(string $name, array $data)`
* `function objectColumn(string $name, object $object)`

### Conditions

* `function all(RowCondition ...$conditions)`
* `function any(RowCondition ...$conditions)`
* `function arrayExists(string $column, string $path)`
* `function arrayValueEquals(string $column, string $path, $value, bool $identical = true)`
* `function arrayValueGreaterOrEqual(string $column, string $path, $value)`
* `function arrayValueGreater(string $column, string $path, $value)`
* `function arrayValueLessOrEqual(string $column, string $path, $value)`
* `function arrayValueLess(string $column, string $path, $value)`
* `function exists(string $column)`
* `function isString(string $column)`
* `function isInteger(string $column)`
* `function isBoolean(string $column)`
* `function isFloat(string $column)`
* `function isArray(string $column)`
* `function isJson(string $column)`
* `function isObject(string $column)`
* `function isNull(string $column)`
* `function isNotNull(string $column)`
* `function valueEquals(string $column, $value, bool $identical = true)`
* `function valueGreaterOrEqual(string $column, $value)`
* `function valueGreater(string $column, $value)`
* `function valueLessOrEqual(string $column, $value)`
* `function valueLess(string $column, $value)`
* `function none(RowCondition $conditions)`
* `function opposite(RowCondition $condition)`

### Extractors 

* `function extractFromCSV(string $fileName, int $batchSize = 100, int $headerOffset = 0)`
* `function extractFromArray(array $array, int $batchSize = 100)`
* `function extractFromJSON(string $fileName, int $batchSize = 100)`
* `function extractFromHttp(ClientInterface $client, iterable $requests, ?callable $preRequest = null, ?callable $postRequest = null)`
* `function extractFromHttpDynamic(ClientInterface $client, NextRequestFactory $requestFactory, ?callable $preRequest = null, ?callable $postRequest = null)`
* `function extractFromDb(Connection $connection, string $query, ParametersSet $parametersSet = null, array $types = [])`

### Factories

* `function rowsFromArray(array $data)`
* `function rowsFromCastedArray(array $data, CastEntry ...$castEntries)`
* `function columnFromValue(string $column, $value)`
 
### Loaders

* `function toCSV(string $fileName)`
* `function toElasticSearch(Client $client, int $chunkSize, string $index, IdFactory $idFactory, array $parameters = [])`
* `function esIdSha1(string ...$columns) :`
* `function esIdColumns(string $column) :`
* `function toMemory(Memory $memory)`
* `function toDebugLogger()`
* `function toColumnDumper(bool $all = false)`
  
### Transformers

* `function filter(string $column, callable $filter)`
* `function filterEquals(string $column, $value)`
* `function filterNotEquals(string $column, $value)`
* `function filterExists(string $column)`
* `function filterNotExists(string $column)`
* `function filterNull(string $column)`
* `function filterNotNull(string $column)`
* `function filterNumber(string $column)`
* `function filterNotNumber(string $column)`
* `function keep(string ...$columns)`
* `function remove(string ...$columns)`
* `function rename(string $from, string $to)`
* `function cloneColumn(string $from, string $to)`
* `function convertName(string $style)`
* `function toDateTime(array $columns, $format = 'c', ?string $tz = null, ?string $toTz = null)`
* `function toDateTimeCast(array $columns, $format = 'c', ?string $tz = null, ?string $toTz = null) : C`
* `function toDate(string ...$columns)`
* `function toDateCast(string ...$columns) : C`
* `function toInteger(string ...$columns)`
* `function toIntegerCast(string ...$columns) : C`
* `function toString(string ...$columns)`
* `function toStringCast(string ...$columns) : C`
* `function toJson(string ...$columns)`
* `function toJsonCast(string ...$columns) : C`
* `function toArrayFromJson(string ...$columns)`
* `function toArrayFromJsonCast(string ...$columns) : C`
* `function toNullFromNullString(string ...$columns)`
* `function toArrayFromObject(string $column)`
* `function expand(string $arrayColumn, string $expandedName = 'column')`
* `function unpack(string $arrayColumn, string $columnPrefix = '', array $skipKeys = [])`
* `function concat(array $stringColumns, string $glue = '', string $columnName = 'column')`
* `function arrayGet(string $arrayName, string $path, string $columnName = 'column')`
* `function objectMethod(string $objectName, string $method, string $columnName = 'column', array $parameters = [])`
* `function addString(string $name, string $value)`
* `function addInteger(string $name, int $value)`
* `function addBoolean(string $name, bool $value)`
* `function addFloat(string $name, float $value)`
* `function addDate(string $name, string $value)`
* `function addDateTime(string $name, string $value, string $format = \DateTimeImmutable::ATOM)`
* `function addArray(string $name, array $data)`
* `function addJson(string $name, array $data)`
* `function addJsonObject(string $name, array $data)`
* `function addObject(string $name, object $data)`
* `function chain(Transformer ...$transformers)`
* `function transformIf(Transformer\Condition\RowCondition $condition, Transformer $transformer)`