--TEST--
NativeParquetReader and NativeParquetWriter leak neither PHP memory nor native allocations, refusals included
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\Parquet\{NativeParquetReader, NativeParquetWriter, SchemaConverter};
use Flow\ETL\Column\DefaultBackend;
use Flow\Parquet\Engine\Arrow\{OptionsConverter, SchemaConverter as ArrowSchemaConverter};
use Flow\Parquet\Options;

use function Flow\ETL\DSL\{int_schema, list_schema, schema, str_schema};
use function Flow\Filesystem\DSL\{memory_filesystem, path};
use function Flow\Types\DSL\{type_list, type_optional, type_string};

$schema = schema(int_schema('id'), str_schema('name', nullable: true), list_schema('tags', type_list(type_optional(type_string()))));
$values = array_map(static fn(int $i): array => ['id' => $i, 'name' => $i % 3 ? "n{$i}" : null, 'tags' => [(string) $i, null]], range(0, 99));
$extension = ArrowSchemaConverter::toExtension((new SchemaConverter())->toParquet($schema));
$options = OptionsConverter::toExtension(Options::default());
// built once: a fresh Type object per cycle would grow flow_php's type-plan cache, which is bounded, not leaked
$mismatched = schema(str_schema('id'));
$invalid = [['id' => 1, 'name' => "\xff", 'tags' => []]];

$cycle = static function () use ($schema, $values, $extension, $options, $mismatched, $invalid): void {
    $filesystem = memory_filesystem();
    $writer = new NativeParquetWriter($filesystem->writeTo(path('memory://leaks.parquet')), $extension, 'SNAPPY', $options);
    $writer->write(native_rows($schema, $values));
    $writer->write(php_rows($schema, $values));
    $writer->close();

    $reader = new NativeParquetReader($filesystem->readFrom(path('memory://leaks.parquet')), $schema, 30, 10, 150);

    while ($reader->next() !== null) {
    }

    $refused = new NativeParquetWriter($filesystem->writeTo(path('memory://refused.parquet')), $extension, 'SNAPPY', $options);

    try {
        $refused->write(native_rows($schema, $invalid));
        throw new LogicException('invalid UTF-8 was written');
    } catch (Flow\ETL\Exception\RuntimeException) {
    }

    try {
        new NativeParquetReader($filesystem->readFrom(path('memory://leaks.parquet')), $mismatched, 30, null, null);
        throw new LogicException('a mismatched schema was accepted');
    } catch (Flow\ETL\Exception\InvalidArgumentException) {
    }
};

for ($i = 0; $i < 10; $i++) {
    $cycle();
}
gc_collect_cycles();
$baseline = memory_get_usage(false);
$baselineRust = (new DefaultBackend())->allocatedBytes();

for ($i = 0; $i < 300; $i++) {
    $cycle();
}
gc_collect_cycles();

var_dump(memory_get_usage(false) <= $baseline);
var_dump((new DefaultBackend())->allocatedBytes() === $baselineRust);
?>
--EXPECT--
bool(true)
bool(true)
