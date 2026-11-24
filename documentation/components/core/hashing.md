# Hashing

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

A hash is 32 hex characters. Equal rows give equal hashes, in any row order and any column order.

| You want the hash of | Use                                        |
|----------------------|--------------------------------------------|
| each row             | `row_hash()`                               |
| each group of rows   | `multiset_hash()`                          |
| the output of a run  | `analyze()->withDatasetHash()`             |
| each partition       | `analyze()->withDatasetHash([ref('...')])` |

## Hash of each row

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, ref, row_hash};

$users = [
    ['id' => 1, 'email' => 'anna@example.com'],
    ['id' => 2, 'email' => 'ben@example.com'],
];

data_frame()
    ->read(from_array($users))
    ->withColumn('row_id', row_hash(ref('id'), ref('email')))
    ->fetch();

// id | email            | row_id
//  1 | anna@example.com | 2744464bb197527041dacd2b68271cb1
//  2 | ben@example.com  | c54423327bcec58acac7706cdbc4272f
```

### Find the rows that changed

A row that changed has a new `row_id`. An anti join keeps the rows of today that have no equal row yesterday:

```php
<?php

use Flow\ETL\Values\Join\JoinType;

use function Flow\ETL\DSL\{data_frame, from_array, join_on, ref, row_hash};

$yesterday = [
    ['id' => 1, 'email' => 'anna@example.com'],
    ['id' => 2, 'email' => 'ben@example.com'],
    ['id' => 3, 'email' => 'cleo@example.com'],
];
$today = [
    ['id' => 1, 'email' => 'anna@example.com'],
    ['id' => 2, 'email' => 'ben@flow-php.com'],
    ['id' => 4, 'email' => 'dan@example.com'],
];

$before = data_frame()
    ->read(from_array($yesterday))
    ->withColumn('row_id', row_hash(ref('id'), ref('email')));

$after = data_frame()
    ->read(from_array($today))
    ->withColumn('row_id', row_hash(ref('id'), ref('email')));

$after->join($before, join_on(['row_id' => 'row_id']), JoinType::left_anti)->fetch();

// id | email            | row_id
//  2 | ben@flow-php.com | f5b67f844cd96344dd9b52ad0c62b296     changed
//  4 | dan@example.com  | e82fadddb3641857a4537cc9227a44cd     new
```

The same join on `id`, from `$before` to `$after`, gives the deleted rows: `id` 3.

## Hash of each group

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, multiset_hash};

$source = [
    ['id' => 1, 'country' => 'PL'],
    ['id' => 2, 'country' => 'DE'],
    ['id' => 3, 'country' => 'PL'],
];
$target = [
    ['id' => 3, 'country' => 'PL'],
    ['id' => 1, 'country' => 'PL'],
    ['id' => 5, 'country' => 'DE'],
];

data_frame()->read(from_array($source))->groupBy('country')->aggregate(multiset_hash())->fetch();

// country | _multiset_hash
// PL      | 2fe815e0c8f7f41d4485efeb669f1a76
// DE      | 01d6077fd12ee579f3a83a381ebfa16f

data_frame()->read(from_array($target))->groupBy('country')->aggregate(multiset_hash())->fetch();

// country | _multiset_hash
// PL      | 2fe815e0c8f7f41d4485efeb669f1a76     equal: same rows, other order
// DE      | bf27be873349d150690543b879826d53     different
```

`multiset_hash()` hashes every column. `multiset_hash(ref('id'))` hashes only `id`.

## Hash of the output of a run

```php
<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\Parquet\{from_parquet, to_parquet};
use function Flow\ETL\DSL\{analyze, data_frame, date_schema, int_schema, schema, str_schema};

$orders = schema(int_schema('id'), date_schema('date'), str_schema('country'));

$written = data_frame()
    ->read(from_csv('orders.csv')->withSchema($orders))
    ->write(to_parquet('orders.parquet'))
    ->run(analyze: analyze()->withDatasetHash())
    ->datasetHash();

$stored = data_frame()
    ->read(from_parquet('orders.parquet'))
    ->run(analyze: analyze()->withDatasetHash())
    ->datasetHash();

$written->algorithm;      // HashAlgorithm::xxh128
$written->total->rows;    // 3
$written->total->value;   // 'f2920d3f120d5130873032ec79839580'
$stored->total->value;    // 'f2920d3f120d5130873032ec79839580', the copy is complete
```

### Choose the algorithm

The default is `xxh128`. A run can use `md5` or `sha256` instead:

```php
<?php

use Flow\ETL\Foundation\Hash\HashAlgorithm;

use function Flow\ETL\DSL\{analyze, data_frame, from_array};

$hash = data_frame()
    ->read(from_array([
        ['id' => 1, 'country' => 'PL'],
        ['id' => 2, 'country' => 'DE'],
        ['id' => 3, 'country' => 'PL'],
    ]))
    ->run(analyze: analyze()->withDatasetHash(algorithm: HashAlgorithm::sha256))
    ->datasetHash();

$hash->algorithm;      // HashAlgorithm::sha256
$hash->total->value;   // 'cb6fbf9a3db645ba28221bc5cc601940'
```

| Algorithm | Hash of the same three rows        |
|-----------|------------------------------------|
| `xxh128`  | `31be1d609a26d996382e2a23855ebbe5` |
| `md5`     | `d3534278fef52e032aa2fe1e55c44c1b` |
| `sha256`  | `cb6fbf9a3db645ba28221bc5cc601940` |

Every hash is 32 hex characters. Flow adds the first 128 bits of the digest of each row, so a `sha256` hash is not
the SHA-256 of a file. The flow_php extension gives the same hash for each algorithm. `row_hash()` and
`multiset_hash()` always use `xxh128`.

## Hash of each partition

```php
<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\{analyze, data_frame, partition_by, ref};

$report = data_frame()
    ->read(from_csv('orders.csv')->withSchema($orders))
    ->write(to_parquet('orders/orders.parquet')->partitionBy(partition_by('date')))
    ->run(analyze: analyze()->withDatasetHash([ref('date')]));

$current = [];

foreach ($report->datasetHash()->partitions as $partition) {
    $current[$partition->partitions->get('date')->segment()] = $partition->hash->value;
}

// [
//     'date=2026-11-20' => '91793f2efee71fbc2caf6b0a8a21a8ff',
//     'date=2026-11-21' => '6118ce10132631745a80c7e1ef61ec81',
// ]
```

`$report->datasetHash()->total` is still the hash of all rows.

### Find the partitions that changed

Store `$current` after each run. In the next run it is `$previous`:

```php
<?php

$changed = array_keys(array_diff_assoc($current, $previous));   // ['date=2026-11-21']
$removed = array_keys(array_diff_key($previous, $current));     // []
```

A null value is the folder `date=__HIVE_DEFAULT_PARTITION__`. An empty string is the folder `date=`.

## Combine runs

`normalize()` gives a hash as an array that you can store. `fromArray()` reads it back. `plus()` adds two hashes:

```php
<?php

use Flow\ETL\Runtime\Metrics\DatasetHash;

file_put_contents('monday.json', json_encode($monday->datasetHash()->normalize()));

// {"algorithm":"xxh128","total":{"rows":1,"value":"61d8d2f77533f36e5f23e4e587fa5e41"},"partitions":[]}

$stored = json_decode(file_get_contents('monday.json'), true);
$week = DatasetHash::fromArray($stored)->plus($tuesday->datasetHash());

$week->total->rows;    // 3
$week->total->value;   // '31be1d609a26d996382e2a23855ebbe5', equal to one run over both days
```

`plus()` also adds the partitions with the same values. If the two hashes have different algorithms, `plus()` throws.

## Rules

- Duplicate rows count: two equal rows give another hash than one.
- Declare the schema. The same value with another type gives another hash.
- A float, list, map, structure, json, xml or html column cannot be a partition column.
- The hash finds accidental differences. It does not protect against tampering.
