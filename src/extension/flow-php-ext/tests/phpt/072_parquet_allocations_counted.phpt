--TEST--
Batches RustParquetOpenSource holds over the flow_php engine are counted by RustBackend::allocatedBytes(): at least their columns' buffer bytes, and nothing once they are released
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\Parquet\IO\Engine\FlowPhpParquetEngine;
use Flow\ETL\Adapter\Parquet\IO\Extractor\RustParquetOpenSource;
use Flow\ETL\Adapter\Parquet\IO\Loader\{ParquetSchemaConformance, RustParquetFileWriter};
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter;
use Flow\ETL\Values\Column\RustBackend;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\Reader\ReadPlan;
use Flow\Parquet\Tests\Double\InMemoryIndexes;

use function Flow\ETL\DSL\{float_schema, int_schema, schema, str_schema};
use function Flow\Filesystem\DSL\{memory_filesystem, path};

$schema = schema(int_schema('id'), str_schema('email'), float_schema('amount', nullable: true));
$parquetSchema = (new SchemaConverter())->toParquetSchema($schema);
$engine = new FlowPhpParquetEngine();
$filesystem = memory_filesystem();
$writer = new RustParquetFileWriter(
    $engine->openForWrite($filesystem->writeTo(path('memory://held.parquet')), $parquetSchema, Compressions::SNAPPY, Options::default()),
    new ParquetSchemaConformance($parquetSchema),
    new PhpBackend(),
);

foreach (array_chunk(range(0, 65_535), 8_192) as $ids) {
    $writer->write(native_rows($schema, array_map(static fn(int $i): array => ['id' => $i, 'email' => "user{$i}@example.com", 'amount' => $i % 5 ? $i / 7 : null], $ids)));
}

$writer->close();
gc_collect_cycles();

$before = (new RustBackend())->allocatedBytes();
$file = $engine->openForRead($filesystem->readFrom(path('memory://held.parquet')));
$reader = (new RustParquetOpenSource($file))->batches(
    $schema,
    1_000,
    ReadPlan::window($file->footer()->rowsNumber(), new InMemoryIndexes()),
    new RustBackend(),
);
$held = [];

foreach ($reader as $batch) {
    $held[] = $batch;
}

unset($reader, $file);
gc_collect_cycles();

$bufferBytes = 0;

foreach ($held as $batch) {
    foreach ($batch->columns() as $column) {
        $bufferBytes += array_sum(array_map(strlen(...), $column->encode()));
    }
}

var_dump(array_sum(array_map(count(...), $held)));
var_dump((new RustBackend())->allocatedBytes() - $before >= $bufferBytes);

unset($held, $batch, $column);
gc_collect_cycles();

var_dump((new RustBackend())->allocatedBytes() === $before);
?>
--EXPECT--
int(65536)
bool(true)
bool(true)
