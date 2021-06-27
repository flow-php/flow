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

Each element of the DSL is a simple php function that can be combined together with other functions. 

### Columns

* `stringColumn(string $name, string $value)`
* `integerColumn(string $name, int $value)`
* `booleanColumn(string $name, bool $value)`
* `floatColumn(string $name, float $value)`
* `dateColumn(string $name, string $value)`
* `dateTimeColumn(string $name, string $value, string $format = \DateTimeImmutable::ATOM)`
* `arrayColumn(string $name, array $data)`
* `jsonColumn(string $name, array $data)`
* `jsonObjectColumn(string $name, array $data)`
* `objectColumn(string $name, object $object)`

### Conditions

* `all(RowCondition ...$conditions)`
* `any(RowCondition ...$conditions)`
* `arrayExists(string $column, string $path)`
* `arrayValueEquals(string $column, string $path, $value, bool $identical = true)`
* `arrayValueGreaterOrEqual(string $column, string $path, $value)`
* `arrayValueGreater(string $column, string $path, $value)`
* `arrayValueLessOrEqual(string $column, string $path, $value)`
* `arrayValueLess(string $column, string $path, $value)`
* `exists(string $column)`
* `isString(string $column)`
* `isInteger(string $column)`
* `isBoolean(string $column)`
* `isFloat(string $column)`
* `isArray(string $column)`
* `isJson(string $column)`
* `isObject(string $column)`
* `isNull(string $column)`
* `isNotNull(string $column)`
* `valueEquals(string $column, $value, bool $identical = true)`
* `valueGreaterOrEqual(string $column, $value)`
* `valueGreater(string $column, $value)`
* `valueLessOrEqual(string $column, $value)`
* `valueLess(string $column, $value)`
* `none(RowCondition $conditions)`
* `opposite(RowCondition $condition)`

### Extractors 

* `extractFromCSV(string $fileName, int $batchSize = 100, int $headerOffset = 0)`
* `extractFromArray(array $array, int $batchSize = 100)`
* `extractFromJSON(string $fileName, int $batchSize = 100)`
* `extractFromHttp(ClientInterface $client, iterable $requests, ?callable $preRequest = null, ?callable $postRequest = null)`
* `extractFromHttpDynamic(ClientInterface $client, NextRequestFactory $requestFactory, ?callable $preRequest = null, ?callable $postRequest = null)`
* `extractFromDb(Connection $connection, string $query, ParametersSet $parametersSet = null, array $types = [])`

### Factories

* `rowsFromArray(array $data)`
* `rowsFromCastedArray(array $data, CastEntry ...$castEntries)`
* `columnFromValue(string $column, $value)`
 
### Loaders

* `toCSV(string $fileName)`
* `toElasticSearch(Client $client, int $chunkSize, string $index, IdFactory $idFactory, array $parameters = [])`
* `esIdSha1(string ...$columns) :`
* `esIdColumns(string $column) :`
* `toMemory(Memory $memory)`
* `toDebugLogger()`
* `toColumnDumper(bool $all = false)`
  
### Transformers

* `filter(string $column, callable $filter)`
* `filterEquals(string $column, $value)`
* `filterNotEquals(string $column, $value)`
* `filterExists(string $column)`
* `filterNotExists(string $column)`
* `filterNull(string $column)`
* `filterNotNull(string $column)`
* `filterNumber(string $column)`
* `filterNotNumber(string $column)`
* `keep(string ...$columns)`
* `remove(string ...$columns)`
* `rename(string $from, string $to)`
* `cloneColumn(string $from, string $to)`
* `convertName(string $style)`
* `toDateTime(array $columns, $format = 'c', ?string $tz = null, ?string $toTz = null)`
* `toDateTimeCast(array $columns, $format = 'c', ?string $tz = null, ?string $toTz = null)`
* `toDate(string ...$columns)`
* `toDateCast(string ...$columns)`
* `toInteger(string ...$columns)`
* `toIntegerCast(string ...$columns)`
* `toString(string ...$columns)`
* `toStringCast(string ...$columns)`
* `toJson(string ...$columns)`
* `toJsonCast(string ...$columns)`
* `toArrayFromJson(string ...$columns)`
* `toArrayFromJsonCast(string ...$columns)`
* `toNullFromNullString(string ...$columns)`
* `toArrayFromObject(string $column)`
* `expand(string $arrayColumn, string $expandedName = 'column')`
* `unpack(string $arrayColumn, string $columnPrefix = '', array $skipKeys = [])`
* `concat(array $stringColumns, string $glue = '', string $columnName = 'column')`
* `arrayGet(string $arrayName, string $path, string $columnName = 'column')`
* `objectMethod(string $objectName, string $method, string $columnName = 'column', array $parameters = [])`
* `addString(string $name, string $value)`
* `addInteger(string $name, int $value)`
* `addBoolean(string $name, bool $value)`
* `addFloat(string $name, float $value)`
* `addDate(string $name, string $value)`
* `addDateTime(string $name, string $value, string $format = \DateTimeImmutable::ATOM)`
* `addArray(string $name, array $data)`
* `addJson(string $name, array $data)`
* `addJsonObject(string $name, array $data)`
* `addObject(string $name, object $data)`
* `chain(Transformer ...$transformers)`
* `transformIf(Transformer\Condition\RowCondition $condition, Transformer $transformer)`