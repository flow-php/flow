--TEST--
NativeParquetReader reads every lib Parquet fixture as PhpParquetEngine does, or refuses it naming the engine that reads it
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\Parquet\{NativeParquetReader, SchemaConverter};
use Flow\ETL\Column\PhpBackend;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Reader;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\{config_builder, flow_context, list_schema, schema, str_schema};
use function Flow\Types\DSL\{type_integer, type_list};
use function Flow\Filesystem\DSL\path;

$fixtures = realpath(__DIR__ . '/../../../../lib/parquet/tests/Flow/Parquet/Tests/Integration/IO/Fixtures');
$filesystem = new NativeLocalFilesystem();
$context = flow_context(config_builder()->backend(new PhpBackend())->build());

foreach ([...glob($fixtures . '/*.parquet'), ...glob($fixtures . '/EdgeCases/*.parquet')] as $file) {
    foreach ([100, 10_000] as $batchSize) {
        $php = outcome(static function () use ($file, $batchSize, $context): array {
            $rows = [];

            foreach (from_parquet($file, engine: new PhpParquetEngine())->withBatchSize($batchSize)->extract($context) as $batch) {
                array_push($rows, ...$batch->toArray());
            }

            return $rows;
        });
        $native = outcome(static function () use ($file, $batchSize, $filesystem): array {
            $reader = new NativeParquetReader(
                $filesystem->readFrom(path($file)),
                (new SchemaConverter())->toFlow((new Reader(engine: new PhpParquetEngine()))->read($file)->schema()),
                $batchSize,
                null,
                null,
            );
            $rows = [];

            while (($batch = $reader->next()) !== null) {
                array_push($rows, ...$batch->toArray());
            }

            return $rows;
        });
        $name = substr($file, strlen($fixtures) + 1) . ' ' . $batchSize;

        echo match (true) {
            $php === $native => "{$name}: identical\n",
            str_contains($native, 'is not supported by the flow_php Parquet reader') => "{$name}: {$native}\n",
            default => "{$name}: DIFFER\n    php:    " . substr($php, 0, 300) . "\n    native: " . substr($native, 0, 300) . "\n",
        };
    }
}

$emptylist = realpath($fixtures . '/EdgeCases/null_list.parquet');
echo outcome(static fn() => new NativeParquetReader(
    $filesystem->readFrom(path($emptylist)),
    schema(list_schema('emptylist', type_list(type_integer()))),
    100,
    null,
    null,
)), "\n";
echo outcome(static fn() => new NativeParquetReader(
    $filesystem->readFrom(path($fixtures . '/multiple_pages.parquet')),
    schema(str_schema('int64')),
    100,
    null,
    null,
)), "\n";
echo outcome(static fn() => new NativeParquetReader(
    $filesystem->readFrom(path($fixtures . '/multiple_pages.parquet')),
    schema(str_schema('missing')),
    100,
    null,
    null,
)), "\n";
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
EdgeCases/nonnullable.impala.parquet 100: identical
EdgeCases/nonnullable.impala.parquet 10000: identical
EdgeCases/null_list.parquet 100: identical
EdgeCases/null_list.parquet 10000: identical
Flow\ETL\Exception\RuntimeException: Parquet column "emptylist" (Null) is not supported by the flow_php Parquet reader; read the file with from_parquet($path, engine: new \Flow\Parquet\Engine\PhpParquetEngine())
Flow\ETL\Exception\InvalidArgumentException: Parquet column "int64" (Int64) is read as Int64, its schema type string stores Binary
Flow\ETL\Exception\InvalidArgumentException: Parquet file has no column "missing"
