--TEST--
RustParquetFileReader, RustColumnsReader and RustParquetFileWriter leak no PHP memory, refusals and Traversable batches included
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Parquet\Engine\Arrow\{OptionsConverter, SchemaConverter};
use Flow\Arrow\Parquet\RustColumnsReader;
use Flow\Parquet\Engine\{RustParquetFileReader, RustParquetFileWriter};
use Flow\Parquet\Options;
use Flow\Parquet\Tests\Context\EveryType;
use Flow\Parquet\ParquetFile\Compressions;

use function Flow\Filesystem\DSL\{memory_filesystem, path};

$schema = SchemaConverter::toExtension(EveryType::schema());
$options = OptionsConverter::toExtension(Options::default());
$rows = EveryType::rows();
$columns = array_map(static fn($column) => $column->name(), EveryType::schema()->columns());

$cycle = static function () use ($schema, $options, $rows, $columns): void {
    $filesystem = memory_filesystem();
    $writer = new RustParquetFileWriter($filesystem->writeTo(path('memory://leaks.parquet')), $schema, Compressions::SNAPPY, $options, 2);
    $writer->writeRows($rows);
    $writer->writeRow($rows[0]);
    $writer->writeBatch((static function () use ($rows): Generator {
        yield from $rows;
    })());
    $writer->writeBatch(new ArrayIterator($rows));

    try {
        $writer->writeRow(['int32' => 'not an int']);
        throw new LogicException('a string was written into INT32');
    } catch (Flow\Parquet\Exception\ValidationException) {
    }

    $writer->close();

    $file = new RustParquetFileReader($filesystem->readFrom(path('memory://leaks.parquet')));
    $file->thrift();
    $file->schema();
    $file->metadata();

    foreach (new RustColumnsReader($file, $columns, 2, 1, 3) as $chunk) {
    }

    foreach ($file->readColumns($columns, 2, 3, 1) as $chunk) {
    }

    try {
        new RustColumnsReader($file, ['missing'], 2, null, null);
        throw new LogicException('a missing column was read');
    } catch (Flow\Parquet\Exception\InvalidArgumentException) {
    }

    $file->close();

    $abandoned = new RustParquetFileWriter($filesystem->writeTo(path('memory://abandoned.parquet')), $schema, Compressions::SNAPPY, $options, 2);
    $abandoned->writeRows($rows);
};

// Schema::fromThrift() / Metadata::fromThrift() warm the allocator up once, whoever calls them
for ($i = 0; $i < 100; $i++) {
    $cycle();
}
gc_collect_cycles();
$baseline = memory_get_usage(false);

for ($i = 0; $i < 200; $i++) {
    $cycle();
}
gc_collect_cycles();

var_dump(memory_get_usage(false) <= $baseline);
?>
--EXPECT--
bool(true)
