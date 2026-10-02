--TEST--
Reader::arrow() on arrow reads every lib Parquet fixture as Reader::php() does, both refuse it, or it refuses naming Reader::php()
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, NestedColumn};
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\EveryType;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Context\ParquetRows;
use Flow\Parquet\Writer;

$fixtures = realpath(__DIR__ . '/../../../../lib/parquet/tests/Flow/Parquet/Tests/Integration/IO/Fixtures');

foreach ([...glob($fixtures . '/*.parquet'), ...glob($fixtures . '/EdgeCases/*.parquet')] as $file) {
    foreach ([100, 10_000] as $batchSize) {
        $php = arrow_outcome(static fn() => ParquetRows::read(Reader::php()->read($file), $batchSize));
        $native = arrow_outcome(static fn() => ParquetRows::read(Reader::arrow()->read($file), $batchSize));
        $name = substr($file, strlen($fixtures) + 1) . ' ' . $batchSize;

        echo match (true) {
            arrow_refused($php) && arrow_refused($native) => "{$name}: both refuse\n",
            $php === $native => "{$name}: identical\n",
            arrow_refused($native) && str_contains($native, 'Reader::php()') => "{$name}: {$native}\n",
            default => "{$name}: DIFFER\n    php:    " . substr($php, 0, 300) . "\n    native: " . substr($native, 0, 300) . "\n",
        };
    }
}

$interval = $fixtures . '/EdgeCases/interval.parquet';
echo 'interval.parquet [id]: ', arrow_outcome(static fn() => ParquetRows::read(Reader::php()->read($interval), 10, ['id'])) === arrow_outcome(static fn() => ParquetRows::read(Reader::arrow()->read($interval), 10, ['id'])) ? 'identical' : 'DIFFER', "\n";

$everyType = MemoryParquetFile::written(Writer::php(), EveryType::schema(), EveryType::rows());
echo 'every type: ', arrow_outcome(static fn() => ParquetRows::read(MemoryParquetFile::read(Reader::php(), $everyType), 2)) === arrow_outcome(static fn() => ParquetRows::read(MemoryParquetFile::read(Reader::arrow(), $everyType), 2)) ? 'identical' : 'DIFFER', "\n";

// pre-epoch timestamps with a sub-second fraction are floored, flat and inside a list
$preEpoch = MemoryParquetFile::written(
    Writer::php(),
    Schema::with(FlatColumn::dateTime('ts'), NestedColumn::list('ts_list', ListElement::datetime())),
    array_map(
        static fn(DateTimeImmutable $at): array => ['ts' => $at, 'ts_list' => [$at]],
        [(new DateTimeImmutable('@0'))->modify('-1500000 microseconds'), (new DateTimeImmutable('@0'))->modify('-1 microsecond'), new DateTimeImmutable('2020-01-02 03:04:05.678901 UTC')],
    ),
);
echo 'pre-epoch timestamps: ', arrow_outcome(static fn() => ParquetRows::read(MemoryParquetFile::read(Reader::php(), $preEpoch), 3)) === arrow_outcome(static fn() => ParquetRows::read(MemoryParquetFile::read(Reader::arrow(), $preEpoch), 3)) ? 'identical' : 'DIFFER', "\n";

foreach (ParquetRows::read(MemoryParquetFile::read(Reader::arrow(), $preEpoch), 3) as $row) {
    echo $row['ts']->format('Y-m-d H:i:s.u'), ' ', $row['ts_list'][0]->format('Y-m-d H:i:s.u'), "\n";
}
?>
--EXPECT--
columns.required.parquet 100: identical
columns.required.parquet 10000: identical
decimals_fixed_len.parquet 100: identical
decimals_fixed_len.parquet 10000: identical
delta_binary_acked_encoded_integers.parquet 100: identical
delta_binary_acked_encoded_integers.parquet 10000: identical
lists.parquet 100: identical
lists.parquet 10000: identical
logical_types.parquet 100: identical
logical_types.parquet 10000: identical
maps.parquet 100: identical
maps.parquet 10000: identical
multiple_pages.parquet 100: identical
multiple_pages.parquet 10000: identical
pagination_row_group_1kb_5k_rows.snappy.parquet 100: identical
pagination_row_group_1kb_5k_rows.snappy.parquet 10000: identical
primitives.parquet 100: identical
primitives.parquet 10000: identical
structs.parquet 100: identical
structs.parquet 10000: identical
EdgeCases/datapage_v2.snappy.parquet 100: identical
EdgeCases/datapage_v2.snappy.parquet 10000: identical
EdgeCases/int96.parquet 100: identical
EdgeCases/int96.parquet 10000: identical
EdgeCases/interval.parquet 100: Flow\Parquet\Exception\RuntimeException: Parquet column "iv" (Interval(DayTime)) is not supported by the arrow Parquet reader; read the file with \Flow\Parquet\Reader::php()
EdgeCases/interval.parquet 10000: Flow\Parquet\Exception\RuntimeException: Parquet column "iv" (Interval(DayTime)) is not supported by the arrow Parquet reader; read the file with \Flow\Parquet\Reader::php()
EdgeCases/map_keys.parquet 100: both refuse
EdgeCases/map_keys.parquet 10000: both refuse
EdgeCases/nonnullable.impala.parquet 100: identical
EdgeCases/nonnullable.impala.parquet 10000: identical
EdgeCases/null_list.parquet 100: identical
EdgeCases/null_list.parquet 10000: identical
EdgeCases/uint64_overflow.parquet 100: both refuse
EdgeCases/uint64_overflow.parquet 10000: both refuse
EdgeCases/unsigned.parquet 100: identical
EdgeCases/unsigned.parquet 10000: identical
interval.parquet [id]: identical
every type: identical
pre-epoch timestamps: identical
1969-12-31 23:59:58.500000 1969-12-31 23:59:58.500000
1969-12-31 23:59:59.999999 1969-12-31 23:59:59.999999
2020-01-02 03:04:05.678901 2020-01-02 03:04:05.678901
