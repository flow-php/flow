--TEST--
RustBackend::allocatedBytes() counts a batch imported from a Parquet read once: it grows with Flow\Arrow\allocated_bytes(), and the rest stays as it was before the read
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
use Flow\ETL\Values\Column\RustBackend;
use Flow\Parquet\Engine\Arrow\{OptionsConverter, SchemaConverter as ArrowSchemaConverter};
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;

use function Flow\ETL\DSL\{int_schema, schema, str_schema};
use function Flow\Filesystem\DSL\{memory_filesystem, path};

$schema = schema(int_schema('id'), str_schema('email'));
$filesystem = memory_filesystem();
$writer = new RustParquetFileWriter(
    new ArrowRustParquetFileWriter(
        $filesystem->writeTo(path('memory://counted.parquet')),
        ArrowSchemaConverter::toExtension((new SchemaConverter())->toParquetSchema($schema)),
        Compressions::SNAPPY,
        OptionsConverter::toExtension(Options::default()),
        1_000,
    ),
    new ParquetSchemaConformance((new SchemaConverter())->toParquetSchema($schema)),
    new PhpBackend(),
);

foreach (array_chunk(range(0, 65_535), 8_192) as $ids) {
    $writer->write(native_rows($schema, array_map(static fn(int $i): array => ['id' => $i, 'email' => "user{$i}@example.com"], $ids)));
}

$writer->close();
gc_collect_cycles();

$backend = new RustBackend();
$arrowBefore = \Flow\Arrow\allocated_bytes();
$restBefore = $backend->allocatedBytes() - $arrowBefore;
$held = iterator_to_array(rust_parquet_batches($filesystem->readFrom(path('memory://counted.parquet')), $schema, 65_536), false);
gc_collect_cycles();

var_dump(array_sum(array_map(count(...), $held)));
var_dump(\Flow\Arrow\allocated_bytes() - $arrowBefore > 1_048_576);
var_dump(abs($backend->allocatedBytes() - \Flow\Arrow\allocated_bytes() - $restBefore) < 1_048_576);
?>
--EXPECT--
int(65536)
bool(true)
bool(true)
