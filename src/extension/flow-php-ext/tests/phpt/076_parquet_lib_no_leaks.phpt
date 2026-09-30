--TEST--
NativeParquetFile, NativeParquetColumnsReader and NativeParquetRowsWriter leak neither PHP memory nor native allocations, refusals included
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Column\DefaultBackend;
use Flow\Parquet\Engine\Arrow\{OptionsConverter, SchemaConverter};
use Flow\Parquet\Engine\Native\{NativeParquetColumnsReader, NativeParquetFile, NativeParquetRowsWriter};
use Flow\Parquet\Options;
use Flow\Parquet\Tests\Context\EveryType;

use function Flow\Filesystem\DSL\{memory_filesystem, path};

$schema = SchemaConverter::toExtension(EveryType::schema());
$options = OptionsConverter::toExtension(Options::default());
$rows = EveryType::rows();
$columns = array_map(static fn($column) => $column->name(), EveryType::schema()->columns());

$cycle = static function () use ($schema, $options, $rows, $columns): void {
    $filesystem = memory_filesystem();
    $writer = new NativeParquetRowsWriter($filesystem->writeTo(path('memory://leaks.parquet')), $schema, 'SNAPPY', $options, 2);
    $writer->writeRows($rows);
    $writer->writeRow($rows[0]);

    try {
        $writer->writeRow(['int32' => 'not an int']);
        throw new LogicException('a string was written into INT32');
    } catch (Flow\Parquet\Exception\ValidationException) {
    }

    $writer->close();

    $file = new NativeParquetFile($filesystem->readFrom(path('memory://leaks.parquet')));
    $file->thrift();
    $file->schema();
    $reader = new NativeParquetColumnsReader($file, $columns, 2, 1, 3);

    while ($reader->next() !== null) {
    }

    try {
        new NativeParquetColumnsReader($file, ['missing'], 2, null, null);
        throw new LogicException('a missing column was read');
    } catch (Flow\Parquet\Exception\InvalidArgumentException) {
    }

    $file->close();

    $abandoned = new NativeParquetRowsWriter($filesystem->writeTo(path('memory://abandoned.parquet')), $schema, 'SNAPPY', $options, 2);
    $abandoned->writeRows($rows);
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
