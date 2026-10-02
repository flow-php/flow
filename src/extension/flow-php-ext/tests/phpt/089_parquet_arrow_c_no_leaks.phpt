--TEST--
Batches crossing to and from arrow-ext through the Arrow C Data Interface leak neither PHP memory nor native allocations: abandoned, never imported, imported twice, refused before and after export, writer dropped unclosed
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
<?php extension_loaded('arrow') || die('skip arrow'); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Arrow\Parquet\{BatchReader, ParquetFile, RowsWriter};
use Flow\ETL\Adapter\Parquet\{NativeParquetWriter, SchemaConverter};
use Flow\ETL\Column\DefaultBackend;
use Flow\Parquet\Engine\Arrow\{OptionsConverter, SchemaConverter as ArrowSchemaConverter};
use Flow\Parquet\Options;

use function Flow\ETL\DSL\{int_schema, list_schema, schema, str_schema};
use function Flow\Filesystem\DSL\{memory_filesystem, path};
use function Flow\Types\DSL\{type_integer, type_list, type_optional};

$schema = schema(int_schema('id'), str_schema('name', nullable: true), list_schema('tags', type_list(type_optional(type_integer())), nullable: true));
$extension = ArrowSchemaConverter::toExtension((new SchemaConverter())->toParquet($schema));
$options = OptionsConverter::toExtension(Options::default());
$values = array_map(static fn(int $i): array => ['id' => $i, 'name' => $i % 3 ? "name {$i}" : null, 'tags' => $i % 4 ? [$i, null] : null], range(0, 99));
// built once: a fresh Type object per cycle would grow flow_php's type-plan cache, which is bounded, not leaked
$rows = native_rows($schema, $values);
$invalid = native_rows($schema, [['id' => 1, 'name' => "\xff", 'tags' => null]]);
$mismatched = schema(str_schema('id'));

$cycle = static function () use ($schema, $extension, $options, $rows, $invalid, $mismatched): void {
    $filesystem = memory_filesystem();
    $writer = new NativeParquetWriter(new RowsWriter($filesystem->writeTo(path('memory://leaks.parquet')), $extension, 'SNAPPY', $options, 30));
    $writer->write($rows);
    $writer->write($rows);
    $writer->close();

    $reader = native_parquet_reader($filesystem->readFrom(path('memory://leaks.parquet')), $schema, 30, null, null);

    while ($reader->next() !== null) {
    }

    $abandoned = native_parquet_reader($filesystem->readFrom(path('memory://leaks.parquet')), $schema, 30, null, null);
    $abandoned->next();
    unset($abandoned);

    $batches = new BatchReader(new ParquetFile($filesystem->readFrom(path('memory://leaks.parquet'))), ['id', 'name', 'tags'], 30, null, null);
    $batches->next();
    $twice = $batches->next();
    $copy = new RowsWriter($filesystem->writeTo(path('memory://copy.parquet')), $extension, 'SNAPPY', $options, 30);
    $copy->writeBatch($twice);

    try {
        $copy->writeBatch($twice);
        throw new LogicException('a batch was imported twice');
    } catch (Flow\Parquet\Exception\InvalidArgumentException) {
    }

    $copy->close();

    try {
        native_parquet_reader($filesystem->readFrom(path('memory://leaks.parquet')), $mismatched, 30, null, null);
        throw new LogicException('a string schema read an int column');
    } catch (Flow\ETL\Exception\InvalidArgumentException) {
    }

    $refused = new NativeParquetWriter(new RowsWriter($filesystem->writeTo(path('memory://refused.parquet')), $extension, 'SNAPPY', $options, 30));

    try {
        $refused->write($invalid);
        throw new LogicException('invalid UTF-8 was written');
    } catch (Flow\Parquet\Exception\RuntimeException) {
    }

    $unclosed = new NativeParquetWriter(new RowsWriter($filesystem->writeTo(path('memory://unclosed.parquet')), $extension, 'SNAPPY', $options, 30));
    $unclosed->write($rows);
};

for ($i = 0; $i < 10; $i++) {
    $cycle();
}
gc_collect_cycles();
$baseline = memory_get_usage(false);
$baselineRust = (new DefaultBackend())->allocatedBytes();

for ($i = 0; $i < 200; $i++) {
    $cycle();
}
gc_collect_cycles();

var_dump(memory_get_usage(false) <= $baseline);
var_dump((new DefaultBackend())->allocatedBytes() === $baselineRust);
?>
--EXPECT--
bool(true)
bool(true)
