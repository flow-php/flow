# Extract Transform Load - Abstraction

[![Minimum PHP Version](https://img.shields.io/badge/php-%3E%3D%207.4-8892BF.svg)](https://php.net/)
[![Latest Stable Version](https://poser.pugx.org/flow-php/etl/v)](https://packagist.org/packages/flow-php/etl)
[![Latest Unstable Version](https://poser.pugx.org/flow-php/etl/v/unstable)](https://packagist.org/packages/flow-php/etl)
[![License](https://poser.pugx.org/flow-php/etl/license)](https://packagist.org/packages/flow-php/etl)
![Tests](https://github.com/flow-php/etl/workflows/Tests/badge.svg?branch=1.x)

## Description

Flow PHP ETL is a simple ETL (Extract Transform Load) abstraction designed to implement Filters & Pipes architecture.

## Installation

```bash
composer require flow-php/etl:1.x@dev
```

## Typical Use Cases

* Sync data from external systems (API)
* File processing
* Pushing data to external systems
* Data migrations

Using this library makes sense when we need to move data from one place to another, doing some transformations in between.

For example, let's say we must synchronize data from external API periodically, transform them into our internal
data structure, filter out things that didn't change, and load in bulk into the database.

This is a perfect scenario for ETL.

## Usage

```php

ETL::extract($extractor)
    ->transform($transformer1)
    ->transform($transformer2)
    ->transform($transformer3)
    ->load($loader)
    ->run();
```

## Features

* Low memory consumption even when processing thousands of records
* Type safe Rows/Row/Entry abstractions
* Filtering
* Built in Rows objects comparison
* Rich collection of Row Entries 

## Row Entries

* [array](src/Flow/ETL/Row/Entry/ArrayEntry.php)
* [boolean](src/Flow/ETL/Row/Entry/BooleanEntry.php)
* [collection](src/Flow/ETL/Row/Entry/CollectionEntry.php)
* [datetime](src/Flow/ETL/Row/Entry/DateTimeEntry.php)
* [float](src/Flow/ETL/Row/Entry/FloatEntry.php)
* [integer](src/Flow/ETL/Row/Entry/IntegerEntry.php)
* [json](src/Flow/ETL/Row/Entry/JsonEntry.php)  
* [null](src/Flow/ETL/Row/Entry/NullEntry.php)
* [object](src/Flow/ETL/Row/Entry/ObjectEntry.php)
* [string](src/Flow/ETL/Row/Entry/StringEntry.php)
* [structure](src/Flow/ETL/Row/Entry/StructureEntry.php)

## Transformers

Set of ETL generic Transformers, for the detailed usage instruction please look into [tests](tests/Flow/ETL/Tests/Unit/Transformer).
Adapters might also define some custom transformers.

* **Generic**
    * [cast](src/Flow/ETL/Transformer/CastTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/CastTransformerTest.php)
    * [chain](src/Flow/ETL/Transformer/ChainTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/ChainTransformerTest.php)
    * [clone entry](src/Flow/ETL/Transformer/CloneEntryTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/CloneEntryTransformerTest.php)
    * [conditional](src/Flow/ETL/Transformer/ConditionalTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/ConditionalTransformerTest.php)
    * [dynamic entry](src/Flow/ETL/Transformer/DynamicEntryTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/DynamicEntryTransformerTest.php)
    * [entry name style converter](src/Flow/ETL/Transformer/EntryNameStyleConverterTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/DynamicEntryTransformerTest.php)
    * [filter rows](src/Flow/ETL/Transformer/FilterRowsTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/FilterRowsTransformerTest.php)
    * [group to array](src/Flow/ETL/Transformer/GroupToArrayTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/GroupToArrayTransformerTest.php)
    * [keep entries](src/Flow/ETL/Transformer/KeepEntriesTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/KeepEntriesTransformerTest.php)
    * [math operation](src/Flow/ETL/Transformer/MathOperationTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/MathOperationTransformerTest.php)
    * [remove entries](src/Flow/ETL/Transformer/RemoveEntriesTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/RemoveEntriesTransformerTest.php)
    * [rename entries](src/Flow/ETL/Transformer/RenameEntriesTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/RenameEntriesTransformerTest.php)
    * [static entry](src/Flow/ETL/Transformer/StaticEntryTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/StaticEntryTransformerTest.php)
* **Array**
    * [array collection get](src/Flow/ETL/Transformer/ArrayCollectionGetTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/ArrayCollectionGetTransformerTest.php)
    * [array collection merge](src/Flow/ETL/Transformer/ArrayCollectionMergeTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/ArrayCollectionMergeTransformerTest.php)
    * [array dot get](src/Flow/ETL/Transformer/ArrayDotGetTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/ArrayDotGetTransformerTest.php)
    * [array rename](src/Flow/ETL/Transformer/ArrayDotRenameTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/ArrayDotRenameTransformerTest.php)
    * [array expand](src/Flow/ETL/Transformer/ArrayExpandTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/ArrayExpandTransformerTest.php)
    * [array keys style converter](src/Flow/ETL/Transformer/ArrayKeysStyleConverterTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/ArrayKeysStyleConverterTransformerTest.php)
    * [array merge](src/Flow/ETL/Transformer/ArrayMergeTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/ArrayMergeTransformerTest.php)
    * [array reverse](src/Flow/ETL/Transformer/ArrayMergeTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/ArrayMergeTransformerTest.php)
    * [array sort](src/Flow/ETL/Transformer/ArraySortTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/ArraySortTransformerTest.php)
    * [array unpack](src/Flow/ETL/Transformer/ArrayUnpackTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/ArrayUnpackTransformerTest.php)
* **Object**
    * [object method](src/Flow/ETL/Transformer/ObjectMethodTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/ObjectMethodTransformerTest.php)
    * [object to array](src/Flow/ETL/Transformer/ObjectToArrayTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/ObjectToArrayTransformerTest.php)
* **String**
    * [null string into null entry](src/Flow/ETL/Transformer/NullStringIntoNullEntryTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/NullStringIntoNullEntryTransformerTest.php)
    * [string concat](src/Flow/ETL/Transformer/StringConcatTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/StringConcatTransformerTest.php)
    * [string entry value case converter](src/Flow/ETL/Transformer/StringEntryValueCaseConverterTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/StringEntryValueCaseConverterTransformerTest.php)
    * [string format](src/Flow/ETL/Transformer/StringFormatTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/StringFormatTransformerTest.php)
* **Callback** - *Might come with performance degradation*
    * [callback entry](src/Flow/ETL/Transformer/CallbackEntryTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/CallbackEntryTransformerTest.php)
    * [callback row](src/Flow/ETL/Transformer/CallbackRowTransformer.php) - [tests](tests/Flow/ETL/Tests/Unit/Transformer/CallbackRowTransformerTest.php)

### Serialization

In order to allow serialization of callable based transformers please
add into your dependencies [opis/closure](https://github.com/opis/closure) library:

```
{
  "require": {
    "opis/closure": "^3.5"
  }
}
```


### Custom Transformer

> If possible it's recommended to avoid writing custom transformers. Official transformers are optimized
> again internal mechanisms which you might not be able to achieve in your custom code.


Custom should only implement `Transformer` interface:

Example:
```php
<?php

use Flow\ETL\Transformer;
use Flow\ETL\Rows;

class NotNorbertTransformer implements Transformer
{
    public function transform(Rows $rows) : Rows
    {
        return $rows->filter(fn(Row $row) => $row->get('name')->value() !== "Norbert");
    }
}
```

### Complex Transformers

Below transformers might not be self descriptive and might require some additional options/dependencies.

#### Transformer - FilterRows

Available Filters

- [all](src/Flow/ETL/Transformer/Filter/Filter/All.php)
- [any](src/Flow/ETL/Transformer/Filter/Filter/Any.php)
- [callback](src/Flow/ETL/Transformer/Filter/Filter/Callback.php)
- [entry equals to](src/Flow/ETL/Transformer/Filter/Filter/EntryEqualsTo.php)
- [entry not equals to](src/Flow/ETL/Transformer/Filter/Filter/EntryNotEqualsTo.php)
- [entry not null](src/Flow/ETL/Transformer/Filter/Filter/EntryNotNull.php)
- [entry not number](src/Flow/ETL/Transformer/Filter/Filter/EntryNotNumber.php)
- [entry number](src/Flow/ETL/Transformer/Filter/Filter/EntryNumber.php)
- [entry exists](src/Flow/ETL/Transformer/Filter/Filter/EntryExists.php)
- [opposite](src/Flow/ETL/Transformer/Filter/Filter/Opposite.php)
- [valid value](src/Flow/ETL/Transformer/Filter/Filter/ValidValue.php) - optionally integrates with [symfony validator](https://github.com/symfony/validator)

#### Transformer - Conditional

Transforms only those Rows that met given condition.

Available Conditions

- [all](src/Flow/ETL/Transformer/Condition/All.php)
- [any](src/Flow/ETL/Transformer/Condition/Any.php)
- [array dot exists](src/Flow/ETL/Transformer/Condition/ArrayDotExists.php)
- [array dot value equals to](src/Flow/ETL/Transformer/Condition/ArrayDotValueEqualsTo.php)
- [array dot value greater or equal than](src/Flow/ETL/Transformer/Condition/ArrayDotValueGreaterOrEqualThan.php)
- [array dot value greater than](src/Flow/ETL/Transformer/Condition/ArrayDotValueGreaterThan.php)
- [array dot value less or equal than](src/Flow/ETL/Transformer/Condition/ArrayDotValueLessOrEqualThan.php)
- [array dot value less than](src/Flow/ETL/Transformer/Condition/ArrayDotValueLessThan.php)
- [entry exists](src/Flow/ETL/Transformer/Condition/EntryExists.php)
- [entry instance of](src/Flow/ETL/Transformer/Condition/EntryInstanceOf.php)
- [entry not null](src/Flow/ETL/Transformer/Condition/EntryNotNull.php)
- [entry value equals to](src/Flow/ETL/Transformer/Condition/EntryValueEqualsTo.php)
- [entry value greater or equal than](src/Flow/ETL/Transformer/Condition/EntryValueGreaterOrEqualThan.php)
- [entry value greater than](src/Flow/ETL/Transformer/Condition/EntryValueGreaterThan.php)
- [entry value less or equal than](src/Flow/ETL/Transformer/Condition/EntryValueLessOrEqualThan.php)
- [entry value less than](src/Flow/ETL/Transformer/Condition/EntryValueLessThan.php)
- [none](src/Flow/ETL/Transformer/Condition/None.php)
- [opposite](src/Flow/ETL/Transformer/Condition/Opposite.php)
- [valid value](src/Flow/ETL/Transformer/Condition/ValidValue) - optionally integrates with [Symfony Validator](https://github.com/symfony/validator)


#### Transformer - Cast


Casting Types:

* [c ast entries](src/Flow/ETL/Transformer/Cast/CastEntries.php)
* [c ast array entry each](src/Flow/ETL/Transformer/Cast/CastArrayEntryEach.php)
* [c ast to datetime](src/Flow/ETL/Transformer/Cast/CastToDateTime.php)
* [c ast to string](src/Flow/ETL/Transformer/Cast/CastToString.php)
* [c ast to integer](src/Flow/ETL/Transformer/Cast/CastToInteger.php)
* [c ast to float](src/Flow/ETL/Transformer/Cast/CastToFloat.php)
* [c ast to json](src/Flow/ETL/Transformer/Cast/CastToJson.php)
* [c ast to array](src/Flow/ETL/Transformer/Cast/CastToArray.php)
* [c ast json to array](src/Flow/ETL/Transformer/Cast/CastJsonToArray.php)

#### Transformer - EntryNameStyleConverter

## Loaders 

In most cases Loaders should be provided by Adapters which you can find below, however there are few generic loaders, 
please find them below.  
Please read [tests](tests/Flow/ETL/Tests/Unit/Loader) to find examples of usage.

* [callback](src/Flow/ETL/Loader/CallbackLoader.php) - [tests](tests/Flow/ETL/Tests/Unit/Loader/CallbackLoaderTest.php)
* [stream](src/Flow/ETL/Loader/StreamLoader.php) - [tests](tests/Flow/ETL/Tests/Unit/Loader/StreamLoaderTest.php)
* [transforming](src/Flow/ETL/Loader/TransformerLoader.php) - [tests](tests/Flow/ETL/Tests/Unit/Loader/TransformerLoaderTest.php)

## Adapters

Adapter connects ETL with existing data sources/storages and including some times custom 
data entries. 

<table style="text-align:center">
<thead>
  <tr>
    <th>Name</th>
    <th>Extractor (read)</th>
    <th>Loader (write)</th>
  </tr>
</thead>
<tbody>
  <tr>
      <td><a href="https://github.com/flow-php/etl-adapter-memory">Memory</a></td>
      <td>✅</td>
      <td>✅</td>
  </tr>
  <tr>
      <td><a href="https://github.com/flow-php/etl-adapter-doctrine">Doctrine - DB</a></td>
      <td>✅</td>
      <td>✅</td>
  </tr>
  <tr>
      <td><a href="https://github.com/flow-php/etl-adapter-elasticsearch">Elasticsearch</a></td>
      <td>N/A</td>
      <td>✅</td>
  </tr>
  <tr>
      <td><a href="https://github.com/flow-php/etl-adapter-csv">CSV</a></td>
      <td>✅</td>
      <td>✅</td>
  </tr>
  <tr>
      <td><a href="https://github.com/flow-php/etl-adapter-json">JSON</a></td>
      <td>✅</td>
      <td>✅</td>
  </tr>
  <tr>
      <td><a href="https://github.com/flow-php/etl-adapter-xml">XML</a></td>
      <td>✅</td>
      <td>N/A</td>
  </tr>
  <tr>
      <td><a href="https://github.com/flow-php/etl-adapter-http">HTTP</a></td>
      <td>✅</td>
      <td>N/A</td>
  </tr>
  <tr>
      <td><a href="#">Excel</a></td>
      <td>N/A</td>
      <td>N/A</td>
  </tr>
  <tr>
      <td><a href="https://github.com/flow-php/etl-adapter-logger">Logger</a></td>
      <td>🚫</td>
      <td>✅</td>
  </tr>
</tbody>
</table>

* ✅ - at least one implementation is available 
* 🚫 - implementation not possible
* `N/A` - not available yet 

**❗ If adapter that you are looking for is not available yet, and you are willing to work on one, feel free to create one as a standalone repository.**
**Well designed and documented adapters can be pulled into `flow-php` organization that will give them maintenance and security support from the organization.** 

## Asynchronous Processing

Flow PHP allows asynchronous processing that can drastically increase processing power.
Asynchronous processing is still under development, the latest progress is available in [flow-php/etl-async](https://github.com/flow-php/etl-async) repository.

## Error Handling 

In case of any exception in transform/load steps, ETL process will break, in order
to change that behavior please set custom [ErrorHandler](src/Flow/ETL/ErrorHandler.php). 

Error Handler defines 3 behavior using 2 methods. 

* `ErrorHandler::throw(\Throwable $error, Rows $rows) : bool`
* `ErrorHandler::skipRows(\Throwable $error, Rows $rows) : bool`

If `throw` returns true, ETL will simply throw an error.
If `skipRows' returns true, ETL will stop processing given rows, and it will try to move to the next batch.
If both methods returns false, ETL will continue processing Rows using next transformers/loaders.

There are 3 build in ErrorHandlers (look for more in adapters):

* [ignore error](src/Flow/ETL/ErrorHandler/IgnoreError.php)
* [skip rows](src/Flow/ETL/ErrorHandler/SkipRows.php)
* [throw error](src/Flow/ETL/ErrorHandler/ThrowError.php)

Error Handling can be set directly at ETL:

```php

ETL::extract($extractor)
    ->onError(new IgnoreError())
    ->transform($transformer)
    ->load($loader)
    ->run();
```

## Collect/Parallelize

```php

ETL::extract($extractor)
    ->transform($transformer1)
    ->transform($transformer2)
    ->collect()
    ->load($loader)
    ->run();
```

Flow PHP ETL is designed to keep memory consumption constant. This can be achieved by processing
only one chunk of data at time.

It's `Extrator` responsibility to define how big those chunks are, for example when processing CSV file with 10k
lines, extractor might want to read only 1k lines at once.

Those 1k lines will be represented as an instance of `Rows`. This means that through ETL pipeline we are
going to push 10 rows, 1k row each.

Main purpose of methods `ETL::collect()` and `ETL::parallelize()` is to adjust number of rows in the middle of processing.

This means that Extractor can still extract 1k rows at once, but before using loader we can use `ETL::collect` which
will wait for all rows to get extracted, then it will merge them and pass total 10k rows into `Loader`.

Parallelize method is exactly opposite, it will not wait for all Rows in order to collect them, instead it will
take any incoming Rows instance and split it into smaller chunks according to `ETL::parallelize(int $chunks)` method `chunks` argument.

```php

ETL::extract($extractor)
    ->transform($transformer1)
    ->transform($transformer2)
    ->load($loader1)
    ->parallelize(20)
    ->transform($transformer3)
    ->transform($transformer4)
    ->load($loader2)
    ->run();
```

## Fetch

Loaders are a great way to load `Rows` into specific Data Sink, however stometimes
you want to simply grab Rows and do something with them. 

```php

$rows = ETL::extract($extractor)
    ->transform($transformer1)
    ->transform($transformer2)
    ->transform($transformer3)
    ->transform($transformer4)
    ->fetch();
```

If `ETL::fetch(int $limit = 0) : Rows` limit argument is different than 0, fetch will
return no more rows than requested. 

## Process

Sometimes you might already have `Rows` prepared, in that case instead of going
through Extractors just use `ETL::process(Rows $rows) : ETL`. 

```php 

ETL::process(new Rows(...))
    ->transform($transformer1)
    ->transform($transformer2)
    ->transform($transformer3)
    ->transform($transformer4)
    ->load($loader)
    ->run();
```

## Display

Display is probably the easiest way to debug ETL's, by default
it will grab selected number of rows (20 by default)

```php

$output = ETL::extract($extractor)
    ->transform($transformer1)
    ->transform($transformer2)
    ->transform($transformer3)
    ->transform($transformer4)
    ->display($limit = 5, $truncate = 0);
    
echo $output;
```

Output:

```
+------+--------+---------+---------------------------+-------+------------------------------+--------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------+
|   id |  price | deleted | created-at                | phase | items                        | tags                                                                                       | object                                                                                         |
+------+--------+---------+---------------------------+-------+------------------------------+--------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------+
| 1234 | 123.45 | false   | 2020-07-13T15:00:00+00:00 | null  | {"item-id":"1","name":"one"} | [{"item-id":"1","name":"one"},{"item-id":"2","name":"two"},{"item-id":"3","name":"three"}] | ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 )) |
| 1234 | 123.45 | false   | 2020-07-13T15:00:00+00:00 | null  | {"item-id":"1","name":"one"} | [{"item-id":"1","name":"one"},{"item-id":"2","name":"two"},{"item-id":"3","name":"three"}] | ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 )) |
| 1234 | 123.45 | false   | 2020-07-13T15:00:00+00:00 | null  | {"item-id":"1","name":"one"} | [{"item-id":"1","name":"one"},{"item-id":"2","name":"two"},{"item-id":"3","name":"three"}] | ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 )) |
| 1234 | 123.45 | false   | 2020-07-13T15:00:00+00:00 | null  | {"item-id":"1","name":"one"} | [{"item-id":"1","name":"one"},{"item-id":"2","name":"two"},{"item-id":"3","name":"three"}] | ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 )) |
| 1234 | 123.45 | false   | 2020-07-13T15:00:00+00:00 | null  | {"item-id":"1","name":"one"} | [{"item-id":"1","name":"one"},{"item-id":"2","name":"two"},{"item-id":"3","name":"three"}] | ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 )) |
+------+--------+---------+---------------------------+-------+------------------------------+--------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------+
5 rows
```

## Performance

The most important thing about performance to remember is that creating custom Loaders/Transformers might have negative impact to
processing performance.

#### ETL::collect()

Using collect on a large number of rows might end up without of memory exception, but it can also significantly increase
loading time into datasink. It might be cheaper to do one big insert than multiple smaller inserts.

## Development

In order to install dependencies please, launch following commands:

```bash
composer install
```

## Run Tests

In order to execute full test suite, please launch following command:

```bash
composer build
```

It's recommended to use [pcov](https://pecl.php.net/package/pcov) for code coverage however you can also use
xdebug by setting `XDEBUG_MODE=coverage` env variable.
