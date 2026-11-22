# Aggregations

[DOC_LINK:/documentation/components/core/group-by.md]

[TOC]

You can aggregate each group of the `groupBy` function with one of the aggregation functions.

## Aggregation functions

- [`COUNT()`](/src/core/etl/src/Flow/ETL/Functions/Count.php)
- [`AVERAGE()`](/src/core/etl/src/Flow/ETL/Functions/Average.php)
- [`SUM()`](/src/core/etl/src/Flow/ETL/Functions/Sum.php)
- [`MIN()`](/src/core/etl/src/Flow/ETL/Functions/Min.php)
- [`MAX()`](/src/core/etl/src/Flow/ETL/Functions/Max.php)
- [`COLLECT()`](/src/core/etl/src/Flow/ETL/Functions/Collect.php)
- [`COLLECT_UNIQUE()`](/src/core/etl/src/Flow/ETL/Functions/CollectUnique.php)
- [`FIRST()`](/src/core/etl/src/Flow/ETL/Functions/First.php)
- [`LAST()`](/src/core/etl/src/Flow/ETL/Functions/Last.php)
- [`STRING_AGG()`](/src/core/etl/src/Flow/ETL/Functions/StringAggregate.php)
- [`MULTISET_HASH()`](/src/core/etl/src/Flow/ETL/Functions/MultisetHash.php)

All aggregation functions implement [`AggregatingFunction`](/src/core/etl/src/Flow/ETL/Functions/AggregatingFunction.php).
`aggregate(Rows $rows, array $indices, Backend $backend)` folds the rows of one group into the state.
`merge(array $partial, Backend $backend)` folds in the `partial()` of another aggregator of the same function.
Group by merges after a spill.

## Custom aggregators

A custom aggregator also describes itself, because a worker builds it again from `describe()`. It gives its state as
a partial: the plain values that a worker returns, so that the parent or a later task can merge them:

```php
<?php

final class LongestString implements AggregatingFunction
{
    private ?string $longest = null;

    public function __construct(private readonly Reference $ref) {}

    public static function fromDescription(array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->of($data['ref'], Reference::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['ref' => $this->ref]);
    }

    public function aggregate(Rows $rows, array $indices, Backend $backend): void
    {
        $column = $rows->column($this->ref->to());

        foreach ($indices as $index) {
            $value = $column->value($index);

            if (is_string($value) && strlen($value) > strlen($this->longest ?? '')) {
                $this->longest = $value;
            }
        }
    }

    public function partialType(): StructureType
    {
        return type_structure(['longest' => type_optional(type_string())]);
    }

    public function partial(): array
    {
        return ['longest' => $this->longest];
    }

    public function merge(array $partial, Backend $backend): void
    {
        $longest = $this->partialType()->assert($partial)['longest'];

        if ($longest !== null && strlen($longest) > strlen($this->longest ?? '')) {
            $this->longest = $longest;
        }
    }

    public function withChildren(array $children): static
    {
        return new self($children[0]);   // a fresh instance: no state crosses
    }

    // children(), resolved(), deterministic(), references(), returns(), outputName(), value() as before
}
```

`partialType()` is valid after the aggregator is resolved. `partial()` of a new instance is the initial state, and a
merge of it is a no-op. Partials of two tasks merge in task order. Order-sensitive aggregators (`first()`, `last()`,
`collect()`) depend on this order. A `describe()` that returns `null` keeps the group by in the parent process.

## Float arithmetic

`sum()` and `average()` add floats as IEEE 754 doubles with compensated summation. `exact: true` changes to decimal
arithmetic:

```php
<?php

use function Flow\ETL\DSL\{df, from_array, ref, sum};

df()->read(from_array([['a' => 0.1], ['a' => 0.2]]))->aggregate([sum(ref('a'))])->fetch();              // 0.30000000000000004
df()->read(from_array([['a' => 0.1], ['a' => 0.2]]))->aggregate([sum(ref('a'), exact: true)])->fetch(); // 0.3
```

## Hashing rows

A sync job writes orders partitioned by day and country. The partition hashes tell which folders changed since the
last run:

```php
<?php

use Flow\ETL\Runtime\Metrics\PartitionHash;
use Flow\Filesystem\Partition;

use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\{analyze, data_frame, date_schema, int_schema, partition_by, ref, schema, str_schema};

$orders = schema(int_schema('id'), date_schema('date'), str_schema('country', nullable: true));

$report = data_frame()
    ->read($externalOrders->withSchema($orders))
    ->write(to_parquet('orders')->partitionBy(partition_by('date', 'country')))
    ->run(analyze: analyze()->withDatasetHash(ref('date'), ref('country')));

$previous = file_exists('orders.hashes.json')
    ? json_decode((string) file_get_contents('orders.hashes.json'), true, flags: JSON_THROW_ON_ERROR)
    : [];
$current = [];

foreach ($report->datasetHash()->partitions as $partition) {
    $segments = array_map(static fn(Partition $p): string => $p->segment(), $partition->partitions->toArray());
    $current[implode('/', $segments)] = $partition->hash->value;
}

$changed = array_keys(array_diff_assoc($current, $previous));   // ['date=2026-11-20/country=PL', ...]
$removed = array_keys(array_diff_key($previous, $current));
file_put_contents('orders.hashes.json', json_encode($current));
```

A partition hash covers every column of every row of the partition. The named columns only group the rows.
`$report->datasetHash()->total` is the hash of all rows: `->value` is the hash and `->rows` is the number of rows.
It equals the hash of `withDatasetHash()` without columns, and then `->partitions` is `[]`. `$partition->hash->rows`
is the number of rows of one partition.

`PartitionHash::normalize()` gives the stored form of one entry,
`['partitions' => ['date' => '2026-11-20', 'country' => 'PL'], 'hash' => ['rows' => 120, 'value' => '...']]`.
`PartitionHash::fromArray()` reads it back.

```php
<?php

use Flow\ETL\Runtime\Metrics\DatasetHash;

$week = DatasetHash::fromArray($storedMondayReport['dataset_hash'])->plus($tuesday->datasetHash());

$week->total->value;   // '31be1d609a26d996382e2a23855ebbe5', the hash of one run over both days
$week->partitions;     // PL: 2 rows, DE: 1 row
```

`plus()` adds the totals and the partitions with the same values. The result equals the hash of one run over the
rows of both.

A null value is the partition `__HIVE_DEFAULT_PARTITION__`, as the loader writes it:
`country=__HIVE_DEFAULT_PARTITION__`.

A list, map, structure, json, xml or html column cannot be named. A float column cannot be named either, because
`0.0` and `-0.0` are one value but two folder names. Cast it first:

```php
<?php

use function Flow\ETL\DSL\{analyze, data_frame, ref};
use function Flow\Types\DSL\type_string;

data_frame()->read($rates)->run(analyze: analyze()->withDatasetHash(ref('rate')));
// analyze()->withDatasetHash() cannot group by the float column "rate": 0.0 and -0.0 are one value but two partition
// names. Cast it to a decimal string or an integer first.

data_frame()
    ->read($rates)
    ->withColumn('rate_key', ref('rate')->cast(type_string()))
    ->run(analyze: analyze()->withDatasetHash(ref('rate_key')));
```

The key column `rate_key` is part of the output, so it is part of every partition hash.

Declare the schema of the partition columns, as in the first sample. Then the hash of a partition is the same between
runs and between workers. The batch size, the order of the files and the number of workers do not change a hash.

```php
<?php

use function Flow\ETL\DSL\{analyze, df, from_array, multiset_hash, ref, row_hash};

$rows = [['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'DE'], ['id' => 3, 'country' => 'PL']];

df()->read(from_array($rows))->withColumn('row_id', row_hash(ref('id'), ref('country')))->fetch();
// row_id: '61d8d2f77533f36e5f23e4e587fa5e41', '01d6077fd12ee579f3a83a381ebfa16f', 'ce0f42e953c400afe5620b05dea4bc35'

df()->read(from_array($rows))->aggregate([multiset_hash()])->fetch();
// _multiset_hash: '31be1d609a26d996382e2a23855ebbe5'

df()->read(from_array($rows))->groupBy('country')->aggregate(multiset_hash(ref('id')))->fetch();
// PL: 'ab8476f29c95212a306bffff897d7afb', DE: 'f3aebd664d56cdeceed04ea04775ad52'

df()->read(from_array($rows))->run(analyze: analyze()->withDatasetHash())->datasetHash()->total->value;
// '31be1d609a26d996382e2a23855ebbe5'
```

Duplicate rows count, and the order of the rows and of the columns does not.

The hash detects accidental differences. It does not resist tampering.

Declare the schema to keep the hash the same across runs: an inferred type can depend on the inference budget.

Json is hashed as its stored text, so whitespace and key order count.

```php
<?php

use function Flow\ETL\DSL\{df, from_array, ref, row_hash};

df()->read(from_array([['id' => 1, 'country' => 'PL']]))->withColumn('row_id', row_hash(ref('id'), ref('countr')))->fetch();
// SchemaDefinitionNotFoundException: Schema definition for column "countr" not found. Did you mean one of: [country]?
```

A named column must exist. `multiset_hash()` without names and the dataset hash take the columns of each batch, and
a column that a batch lacks hashes like a null.

`hash()` is the digest of the text of one value. `row_hash()` is the typed hash of the named columns:

```php
<?php

use function Flow\ETL\DSL\{df, from_array, hash, hash_algorithm, ref};

df()->read(from_array([['country' => 'PL']]))->withColumn('digest', hash(ref('country'), 'sha256'))->fetch();
// '293650de072648b9e2790509d8109d0f34f655edfe0b20075b0eaf6a93e95646'

df()->read(from_array([['country' => 'PL']]))
    ->withColumn('digest', ref('country')->hash(hash_algorithm('xxh64', options: ['seed' => 7])))
    ->fetch();
// '30aa8eee14870cd7'
```

## Example

```php
<?php

use Flow\ETL\Foundation\Calculator\Rounding;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{average, concat, data_frame, lit, max, min, ref, sum, to_output};

data_frame()
    ->read(from_csv(__DIR__ . '/power-plant-daily.csv', separator: ';'))
    ->withColumn('production_kwh', ref('Produkcja(kWh)'))
    ->withColumn('consumption_kwh', ref('Zużycie(kWh)'))
    ->withColumn('date', ref('Zaktualizowany czas')->toDate('Y/m/d')->dateFormat('Y/m'))
    ->select('date', 'production_kwh', 'consumption_kwh')
    ->groupBy([ref('date')])
    ->aggregate(
        average(ref('production_kwh')),
        average(ref('consumption_kwh')),
        min(ref('production_kwh')),
        min(ref('consumption_kwh')),
        max(ref('production_kwh')),
        max(ref('consumption_kwh')),
        sum(ref('production_kwh')),
        sum(ref('consumption_kwh'))
    )
    ->withColumn('production_kwh_avg', ref('production_kwh_avg')->round(lit(2)))
    ->withColumn('consumption_kwh_avg', ref('consumption_kwh_avg')->round(lit(2)))
    ->withColumn('production_kwh_min', ref('production_kwh_min')->round(lit(2)))
    ->withColumn('consumption_kwh_min', ref('consumption_kwh_min')->round(lit(2)))
    ->withColumn('production_kwh_max', ref('production_kwh_max')->round(lit(2)))
    ->withColumn('consumption_kwh_max', ref('consumption_kwh_max')->round(lit(2)))
    ->withColumn('production_kwh_sum', ref('production_kwh_sum')->round(lit(2)))
    ->withColumn('consumption_kwh_sum', ref('consumption_kwh_sum')->round(lit(2)))
    ->withColumn('consumption', ref('consumption_kwh_sum')->divide(ref('production_kwh_sum'), 4, Rounding::HALF_UP))
    ->withColumn('consumption', ref('consumption')->multiply(lit(100))->round(lit(2)))
    ->withColumn('consumption', concat(ref('consumption'), lit('%')))
    ->write(to_output(truncate: false))
    ->run();
```