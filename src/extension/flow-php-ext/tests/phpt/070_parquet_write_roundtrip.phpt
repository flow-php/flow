--TEST--
RustParquetOpenSink writes native and PHP columns that PhpParquetEngine reads back equal, and refuses a string that is not valid UTF-8
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
<?php extension_loaded('arrow') || die('skip arrow'); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Parquet\Engine\RustParquetFileWriter;

use Flow\ETL\Adapter\Parquet\{RustParquetOpenSink, SchemaConverter};
use Flow\ETL\Column\PhpBackend;
use Flow\Parquet\Engine\Arrow\{OptionsConverter, SchemaConverter as ArrowSchemaConverter};
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Options;
use Flow\ETL\Schema;
use Flow\Types\Value\Uuid;
use Flow\Parquet\ParquetFile\Compressions;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\{bool_schema, config_builder, date_schema, datetime_schema, float_schema, flow_context, int_schema, json_schema, list_schema, map_schema, schema, str_schema, structure_schema, time_schema, uuid_schema};
use function Flow\Filesystem\DSL\{memory_filesystem, path};
use function Flow\Types\DSL\{type_integer, type_list, type_map, type_optional, type_string, type_structure};

$schema = schema(
    int_schema('int'),
    str_schema('string', nullable: true),
    float_schema('float'),
    bool_schema('bool'),
    datetime_schema('datetime'),
    date_schema('date'),
    time_schema('time', nullable: true),
    uuid_schema('uuid'),
    json_schema('json'),
    list_schema('list', type_list(type_optional(type_integer()))),
    map_schema('map', type_map(type_string(), type_integer())),
    structure_schema('structure', type_structure(['id' => type_integer(), 'name' => type_optional(type_string())])),
);
$values = [
    [
        'int' => -7, 'string' => 'zażółć', 'float' => 0.14, 'bool' => true,
        'datetime' => new DateTimeImmutable('1969-12-31 23:59:59.999999 UTC'), 'date' => new DateTimeImmutable('1900-01-01'),
        'time' => new DateInterval('PT1H2M3S'), 'uuid' => new Uuid('26fd21b0-6080-4d6c-bdb4-1214f1feffef'),
        'json' => '{"a":[1,2]}', 'list' => [1, null, 3], 'map' => ['a' => 1, 'b' => 2], 'structure' => ['id' => 1, 'name' => null],
    ],
    [
        'int' => PHP_INT_MAX, 'string' => null, 'float' => 1234567890123456.8, 'bool' => false,
        'datetime' => new DateTimeImmutable('2026-09-29 12:00:00.5 UTC'), 'date' => new DateTimeImmutable('2026-09-29'),
        'time' => null, 'uuid' => new Uuid('00000000-0000-0000-0000-000000000000'),
        'json' => '[]', 'list' => [], 'map' => [], 'structure' => ['id' => 2, 'name' => 'x'],
    ],
];
$context = flow_context(config_builder()->backend(new PhpBackend())->build());
$write = static function (Schema $schema, array $batches): void {
    $writer = new RustParquetOpenSink(new RustParquetFileWriter(
        memory_filesystem()->writeTo(path('memory://unused.parquet')),
        ArrowSchemaConverter::toExtension((new SchemaConverter())->toParquet($schema)),
        Compressions::ZSTD,
        OptionsConverter::toExtension(Options::default()),
        1_000,
    ));

    foreach ($batches as $batch) {
        $writer->write($batch);
    }

    $writer->close();
};

$filesystem = memory_filesystem();
$writer = new RustParquetOpenSink(new RustParquetFileWriter(
    $filesystem->writeTo(path('memory://roundtrip.parquet')),
    ArrowSchemaConverter::toExtension((new SchemaConverter())->toParquet($schema)),
    Compressions::ZSTD,
    OptionsConverter::toExtension(Options::default()),
    1_000,
));
$writer->write(native_rows($schema, $values));
$writer->write(php_rows($schema, $values));
$writer->close();

$read = [];

foreach (from_parquet(path('memory://roundtrip.parquet'), filesystem: $filesystem, engine: new PhpParquetEngine())->extract($context) as $batch) {
    array_push($read, ...$batch->toArray());
}

var_dump(comparable($read) === comparable([...php_rows($schema, $values)->toArray(), ...php_rows($schema, $values)->toArray()]));

$partial = memory_filesystem();
$partialWriter = new RustParquetOpenSink(new RustParquetFileWriter(
    $partial->writeTo(path('memory://partial.parquet')),
    ArrowSchemaConverter::toExtension((new SchemaConverter())->toParquet(schema(int_schema('id'), str_schema('name', nullable: true)))),
    Compressions::SNAPPY,
    OptionsConverter::toExtension(Options::default()),
    1_000,
));
$partialWriter->write(native_rows(schema(int_schema('id')), [['id' => 1], ['id' => 2]]));
$partialWriter->close();
$partialRead = [];

foreach (from_parquet(path('memory://partial.parquet'), filesystem: $partial, engine: new PhpParquetEngine())->extract($context) as $batch) {
    array_push($partialRead, ...$batch->toArray());
}

echo json_encode($partialRead), "\n";

$names = schema(str_schema('name'));
echo outcome(static fn() => $write($names, [native_rows($names, [['name' => 'ok'], ['name' => "\xff\xfe"]])])), "\n";
echo outcome(static fn() => $write($names, [php_rows($names, [['name' => "\xff\xfe"]])])), "\n";

$nested = schema(list_schema('tags', type_list(type_string())));
echo outcome(static fn() => $write($nested, [native_rows($nested, [['tags' => ['a']], ['tags' => ['b', "\xff"]]])])), "\n";
?>
--EXPECT--
bool(true)
[{"id":1,"name":null},{"id":2,"name":null}]
Flow\Parquet\Exception\RuntimeException: Parquet column "name" row 1 holds a string that is not valid UTF-8; Parquet STRING columns require UTF-8
Flow\Parquet\Exception\RuntimeException: Parquet column "name" row 0 holds a string that is not valid UTF-8; Parquet STRING columns require UTF-8
Flow\Parquet\Exception\RuntimeException: Parquet column "tags" row 1 holds a string that is not valid UTF-8; Parquet STRING columns require UTF-8
