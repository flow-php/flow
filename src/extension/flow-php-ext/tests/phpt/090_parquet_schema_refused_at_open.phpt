--TEST--
NativeParquetReader refuses a schema type that stores another arrow type at construction, before any batch: over a zero-row file and past the end of an 8-row-group file
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
<?php extension_loaded('arrow') || die('skip arrow'); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Arrow\Parquet\RowsWriter;
use Flow\ETL\Adapter\Parquet\{NativeParquetWriter, SchemaConverter};
use Flow\Parquet\Engine\Arrow\{OptionsConverter, SchemaConverter as ArrowSchemaConverter};
use Flow\Parquet\{Option, Options};

use function Flow\ETL\DSL\{array_to_rows, int_schema, schema, str_schema};
use function Flow\Filesystem\DSL\{memory_filesystem, path};

$filesystem = memory_filesystem();
$schema = schema(int_schema('id'));
$extension = ArrowSchemaConverter::toExtension((new SchemaConverter())->toParquet($schema));

$empty = new RowsWriter($filesystem->writeTo(path('memory://empty.parquet')), $extension, 'SNAPPY', OptionsConverter::toExtension(Options::default()), 10);
$empty->close();

$groups = new NativeParquetWriter(new RowsWriter(
    $filesystem->writeTo(path('memory://groups.parquet')),
    $extension,
    'SNAPPY',
    OptionsConverter::toExtension(Options::default()->set(Option::ROW_GROUP_SIZE_BYTES, 1)),
    10,
));

for ($group = 0; $group < 8; $group++) {
    $groups->write(array_to_rows(array_map(static fn(int $id): array => ['id' => $id], range($group * 10, $group * 10 + 9)), $schema));
}

$groups->close();

$mismatched = schema(str_schema('id'));
echo outcome(static fn() => native_parquet_reader($filesystem->readFrom(path('memory://empty.parquet')), $mismatched, 10, null, null)), "\n";
echo outcome(static fn() => native_parquet_reader($filesystem->readFrom(path('memory://groups.parquet')), $mismatched, 10, 1_000, null)), "\n";
?>
--EXPECT--
Flow\ETL\Exception\InvalidArgumentException: Parquet column "id" (Int64) is read as Int64, its schema type string stores Binary
Flow\ETL\Exception\InvalidArgumentException: Parquet column "id" (Int64) is read as Int64, its schema type string stores Binary
