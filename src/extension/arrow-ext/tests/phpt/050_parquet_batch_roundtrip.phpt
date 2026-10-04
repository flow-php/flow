--TEST--
RustBatchReader batches written back through RustParquetFileWriter::writeArrowBatch() read as the file; writer columns the batch lacks are nulls, children the writer lacks are ignored, a batch imports once, userland objects are refused, nothing leaks
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Arrow\Parquet\RustBatchReader;
use Flow\Parquet\Engine\{RustParquetFileReader, RustParquetFileWriter};
use Flow\Filesystem\Filesystem;
use Flow\Parquet\Engine\Arrow\{OptionsConverter, SchemaConverter};
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, NestedColumn};
use Flow\Parquet\Reader;
use Flow\Parquet\ParquetFile\Compressions;

use function Flow\Filesystem\DSL\{memory_filesystem, path};

final class UserlandBatch
{
    public function arrowSchemaAddress(): int
    {
        return 1;
    }

    public function arrowArrayAddress(): int
    {
        return 1;
    }
}

$source = Schema::with(FlatColumn::int64('id'), FlatColumn::string('name'), NestedColumn::list('tags', ListElement::int64()));
$target = Schema::with(FlatColumn::int64('id'), FlatColumn::string('name'), FlatColumn::string('missing'));
$options = OptionsConverter::toExtension(Options::default());
$rows = array_map(static fn(int $i): array => ['id' => $i, 'name' => $i % 3 ? "name {$i}" : null, 'tags' => $i % 4 ? [$i, $i + 1] : null], range(0, 249));

$roundTrip = static function (Filesystem $filesystem) use ($source, $target, $options, $rows): array {
    $writer = new RustParquetFileWriter($filesystem->writeTo(path('memory://source.parquet')), SchemaConverter::toExtension($source), Compressions::SNAPPY, $options, 100);
    $writer->writeRows($rows);
    $writer->close();

    $batches = new RustBatchReader(new RustParquetFileReader($filesystem->readFrom(path('memory://source.parquet'))), ['id', 'name', 'tags'], 100, null, null);
    $schema = $batches->schema();
    $copy = new RustParquetFileWriter($filesystem->writeTo(path('memory://copy.parquet')), SchemaConverter::toExtension($target), Compressions::SNAPPY, $options, 100);
    $counts = [];

    while (($batch = $batches->next()) !== null) {
        $counts[] = $batch->count();
        $copy->writeArrowBatch($batch);
    }

    $copy->close();

    return [$schema, $counts];
};

$filesystem = memory_filesystem();
[$schema, $counts] = $roundTrip($filesystem);
$expected = array_map(static fn(array $row): array => ['id' => $row['id'], 'name' => $row['name'], 'missing' => null], $rows);

echo get_class($schema), ' ', $schema->arrowSchemaAddress() > 0 ? 'has an address' : 'NO ADDRESS', "\n";
echo 'batches: ', implode(',', $counts), "\n";
echo 'round trip: ', iterator_to_array(Reader::php()->readStream($filesystem->readFrom(path('memory://copy.parquet')))->values(), false) === $expected ? 'identical' : 'DIFFER', "\n";

$batches = new RustBatchReader(new RustParquetFileReader($filesystem->readFrom(path('memory://source.parquet'))), ['id', 'name', 'tags'], 100, null, null);
$batch = $batches->next();
$twice = new RustParquetFileWriter($filesystem->writeTo(path('memory://twice.parquet')), SchemaConverter::toExtension($target), Compressions::SNAPPY, $options, 100);
$twice->writeArrowBatch($batch);
echo arrow_outcome(static fn() => $twice->writeArrowBatch($batch)), "\n";
echo arrow_outcome(static fn() => $twice->writeArrowBatch(new UserlandBatch())), "\n";
$twice->close();

for ($i = 0; $i < 10; $i++) {
    $roundTrip(memory_filesystem());
}
gc_collect_cycles();
$baseline = memory_get_usage(false);

for ($i = 0; $i < 100; $i++) {
    $roundTrip(memory_filesystem());
}
gc_collect_cycles();

var_dump(memory_get_usage(false) <= $baseline);
?>
--EXPECT--
Flow\Arrow\RustArrowSchema has an address
batches: 100,100,50
round trip: identical
Flow\Parquet\Exception\InvalidArgumentException: Arrow C Data batch is already imported
Flow\Parquet\Exception\InvalidArgumentException: Arrow C Data batch must be an extension's class, got UserlandBatch
bool(true)
