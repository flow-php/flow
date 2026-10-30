--TEST--
RustParquetOpenSource refuses a schema type that stores another arrow type when batches() is called, before any batch: over a zero-row file and past the end of an 8-row-group file; a file other than RustParquetFileReader and a writer other than RustParquetFileWriter are refused
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
<?php extension_loaded('arrow') || die('skip arrow'); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Parquet\Engine\RustParquetFileWriter as ArrowRustParquetFileWriter;
use Flow\ETL\Adapter\Parquet\IO\Loader\{ParquetSchemaConformance, RustParquetFileWriter};
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter;
use Flow\Parquet\Engine\Arrow\{OptionsConverter, SchemaConverter as ArrowSchemaConverter};
use Flow\Parquet\{Option, Options};
use Flow\Parquet\ParquetFile\Compressions;

use function Flow\ETL\DSL\{array_to_rows, int_schema, schema, str_schema};
use function Flow\Filesystem\DSL\{memory_filesystem, path};

$filesystem = memory_filesystem();
$schema = schema(int_schema('id'));
$extension = ArrowSchemaConverter::toExtension((new SchemaConverter())->toParquetSchema($schema));

$empty = new ArrowRustParquetFileWriter($filesystem->writeTo(path('memory://empty.parquet')), $extension, Compressions::SNAPPY, OptionsConverter::toExtension(Options::default()), 10);
$empty->close();

$groups = new RustParquetFileWriter(
    new ArrowRustParquetFileWriter(
        $filesystem->writeTo(path('memory://groups.parquet')),
        $extension,
        Compressions::SNAPPY,
        OptionsConverter::toExtension(Options::default()->set(Option::ROW_GROUP_SIZE_BYTES, 1)),
        10,
    ),
    new ParquetSchemaConformance((new SchemaConverter())->toParquetSchema($schema)),
    new PhpBackend(),
);

for ($group = 0; $group < 8; $group++) {
    $groups->write(array_to_rows(array_map(static fn(int $id): array => ['id' => $id], range($group * 10, $group * 10 + 9)), $schema));
}

$groups->close();

$mismatched = schema(str_schema('id'));
echo outcome(static fn() => rust_parquet_batches($filesystem->readFrom(path('memory://empty.parquet')), $mismatched, 10)), "\n";
echo outcome(static fn() => rust_parquet_batches($filesystem->readFrom(path('memory://groups.parquet')), $mismatched, 10, 1_000)), "\n";
echo outcome(static fn() => new Flow\ETL\Adapter\Parquet\IO\Extractor\RustParquetOpenSource(new stdClass())), "\n";
echo outcome(static fn() => new Flow\ETL\Adapter\Parquet\IO\Loader\RustParquetFileWriter(new stdClass(), new stdClass(), new stdClass())), "\n";
?>
--EXPECT--
Flow\ETL\Foundation\Exception\InvalidArgumentException: Parquet column "id" (Int64) is read as Int64, its schema type string stores LargeBinary
Flow\ETL\Foundation\Exception\InvalidArgumentException: Parquet column "id" (Int64) is read as Int64, its schema type string stores LargeBinary
Flow\ETL\Foundation\Exception\RuntimeException: flow_php expected a Flow\Parquet\Engine\RustParquetFileReader
Flow\ETL\Foundation\Exception\RuntimeException: flow_php expected a Flow\Parquet\Engine\RustParquetFileWriter
