--TEST--
RustParquetOpenSource and RustParquetFileWriter leak neither PHP memory nor native allocations, refusals included
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

use function Flow\ETL\DSL\{int_schema, list_schema, schema, str_schema};
use function Flow\Filesystem\DSL\{memory_filesystem, path};
use function Flow\Types\DSL\{type_list, type_optional, type_string};

$schema = schema(int_schema('id'), str_schema('name', nullable: true), list_schema('tags', type_list(type_optional(type_string()))));
$values = array_map(static fn(int $i): array => ['id' => $i, 'name' => $i % 3 ? "n{$i}" : null, 'tags' => [(string) $i, null]], range(0, 99));
$parquetSchema = (new SchemaConverter())->toParquetSchema($schema);
$extension = ArrowSchemaConverter::toExtension($parquetSchema);
$conformance = new ParquetSchemaConformance($parquetSchema);
$options = OptionsConverter::toExtension(Options::default());
// built once: a fresh Type object per cycle would grow flow_php's type-plan cache, which is bounded, not leaked
$mismatched = schema(str_schema('id'));
$invalid = [['id' => 1, 'name' => "\xff", 'tags' => []]];

$cycle = static function () use ($schema, $values, $extension, $conformance, $options, $mismatched, $invalid): void {
    $filesystem = memory_filesystem();
    $writer = new RustParquetFileWriter(new ArrowRustParquetFileWriter($filesystem->writeTo(path('memory://leaks.parquet')), $extension, Compressions::SNAPPY, $options, 1_000), $conformance, new PhpBackend());
    $writer->write(native_rows($schema, $values));
    $writer->write(php_rows($schema, $values));
    $writer->close();

    $reader = rust_parquet_batches($filesystem->readFrom(path('memory://leaks.parquet')), $schema, 30, 10, 150);

    foreach ($reader as $_) {
    }

    $refused = new RustParquetFileWriter(new ArrowRustParquetFileWriter($filesystem->writeTo(path('memory://refused.parquet')), $extension, Compressions::SNAPPY, $options, 1_000), $conformance, new PhpBackend());

    try {
        $refused->write(native_rows($schema, $invalid));
        throw new LogicException('invalid UTF-8 was written');
    } catch (Flow\Parquet\Exception\RuntimeException) {
    }

    try {
        rust_parquet_batches($filesystem->readFrom(path('memory://leaks.parquet')), $mismatched, 30);
        throw new LogicException('a mismatched schema was accepted');
    } catch (Flow\ETL\Foundation\Exception\InvalidArgumentException) {
    }
};

for ($i = 0; $i < 10; $i++) {
    $cycle();
}
gc_collect_cycles();
$baseline = memory_get_usage(false);
$baselineRust = (new RustBackend())->allocatedBytes();

for ($i = 0; $i < 300; $i++) {
    $cycle();
}
gc_collect_cycles();

var_dump(memory_get_usage(false) <= $baseline);
var_dump((new RustBackend())->allocatedBytes() === $baselineRust);
?>
--EXPECT--
bool(true)
bool(true)
