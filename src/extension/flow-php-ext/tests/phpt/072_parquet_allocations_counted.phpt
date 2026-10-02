--TEST--
Batches NativeParquetReader holds are counted by DefaultBackend::allocatedBytes(): at least their columns' buffer bytes, and nothing once they are released
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
<?php extension_loaded('arrow') || die('skip arrow'); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Arrow\Parquet\RowsWriter;

use Flow\ETL\Adapter\Parquet\{NativeParquetReader, NativeParquetWriter, SchemaConverter};
use Flow\ETL\Column\DefaultBackend;
use Flow\Parquet\Engine\Arrow\{OptionsConverter, SchemaConverter as ArrowSchemaConverter};
use Flow\Parquet\Options;

use function Flow\ETL\DSL\{float_schema, int_schema, schema, str_schema};
use function Flow\Filesystem\DSL\{memory_filesystem, path};

$schema = schema(int_schema('id'), str_schema('email'), float_schema('amount', nullable: true));
$filesystem = memory_filesystem();
$writer = new NativeParquetWriter(new RowsWriter(
    $filesystem->writeTo(path('memory://held.parquet')),
    ArrowSchemaConverter::toExtension((new SchemaConverter())->toParquet($schema)),
    'SNAPPY',
    OptionsConverter::toExtension(Options::default()),
    1_000,
));

foreach (array_chunk(range(0, 65_535), 8_192) as $ids) {
    $writer->write(native_rows($schema, array_map(static fn(int $i): array => ['id' => $i, 'email' => "user{$i}@example.com", 'amount' => $i % 5 ? $i / 7 : null], $ids)));
}

$writer->close();
gc_collect_cycles();

$before = (new DefaultBackend())->allocatedBytes();
$reader = native_parquet_reader($filesystem->readFrom(path('memory://held.parquet')), $schema, 1_000, null, null);
$held = [];

while (($batch = $reader->next()) !== null) {
    $held[] = $batch;
}

$reader->close();
unset($reader);
gc_collect_cycles();

$bufferBytes = 0;

foreach ($held as $batch) {
    foreach ($batch->columns() as $column) {
        $bufferBytes += array_sum(array_map(strlen(...), $column->encode()));
    }
}

var_dump(array_sum(array_map(count(...), $held)));
var_dump((new DefaultBackend())->allocatedBytes() - $before >= $bufferBytes);

unset($held, $batch, $column);
gc_collect_cycles();

var_dump((new DefaultBackend())->allocatedBytes() === $before);
?>
--EXPECT--
int(65536)
bool(true)
bool(true)
