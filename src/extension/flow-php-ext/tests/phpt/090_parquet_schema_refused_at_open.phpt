--TEST--
RustParquetOpenSource refuses a schema type that stores another arrow type when batches() is called, before any batch: over a zero-row file and past the end of an 8-row-group file; a file other than RustParquetFileReader and a writer other than RustParquetFileWriter are refused
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
<?php extension_loaded('arrow') || die('skip arrow'); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Parquet\Engine\RustParquetFileWriter;
use Flow\ETL\Adapter\Parquet\{RustParquetOpenSink, SchemaConverter};
use Flow\Parquet\Engine\Arrow\{OptionsConverter, SchemaConverter as ArrowSchemaConverter};
use Flow\Parquet\{Option, Options};
use Flow\Parquet\ParquetFile\Compressions;

use function Flow\ETL\DSL\{array_to_rows, int_schema, schema, str_schema};
use function Flow\Filesystem\DSL\{memory_filesystem, path};

$filesystem = memory_filesystem();
$schema = schema(int_schema('id'));
$extension = ArrowSchemaConverter::toExtension((new SchemaConverter())->toParquet($schema));

$empty = new RustParquetFileWriter($filesystem->writeTo(path('memory://empty.parquet')), $extension, Compressions::SNAPPY, OptionsConverter::toExtension(Options::default()), 10);
$empty->close();

$groups = new RustParquetOpenSink(new RustParquetFileWriter(
    $filesystem->writeTo(path('memory://groups.parquet')),
    $extension,
    Compressions::SNAPPY,
    OptionsConverter::toExtension(Options::default()->set(Option::ROW_GROUP_SIZE_BYTES, 1)),
    10,
));

for ($group = 0; $group < 8; $group++) {
    $groups->write(array_to_rows(array_map(static fn(int $id): array => ['id' => $id], range($group * 10, $group * 10 + 9)), $schema));
}

$groups->close();

$mismatched = schema(str_schema('id'));
echo outcome(static fn() => rust_parquet_batches($filesystem->readFrom(path('memory://empty.parquet')), $mismatched, 10, null, null)), "\n";
echo outcome(static fn() => rust_parquet_batches($filesystem->readFrom(path('memory://groups.parquet')), $mismatched, 10, 1_000, null)), "\n";
echo outcome(static fn() => new Flow\ETL\Adapter\Parquet\RustParquetOpenSource(new stdClass())), "\n";
echo outcome(static fn() => new Flow\ETL\Adapter\Parquet\RustParquetOpenSink(new stdClass())), "\n";
?>
--EXPECT--
Flow\ETL\Exception\InvalidArgumentException: Parquet column "id" (Int64) is read as Int64, its schema type string stores LargeBinary
Flow\ETL\Exception\InvalidArgumentException: Parquet column "id" (Int64) is read as Int64, its schema type string stores LargeBinary
Flow\ETL\Exception\RuntimeException: flow_php expected a Flow\Parquet\Engine\RustParquetFileReader
Flow\ETL\Exception\RuntimeException: flow_php expected a Flow\Parquet\Engine\RustParquetFileWriter
