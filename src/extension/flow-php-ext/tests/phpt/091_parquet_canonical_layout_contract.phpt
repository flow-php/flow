--TEST--
arrow-ext's canonical Arrow layout (parquet/canonical.rs) is the layout flow_php's columns store (flow-batch-frame kind::data_type()): every Flow type, nested included, crosses BatchReader → NativeParquetReader with its values intact
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
<?php extension_loaded('arrow') || die('skip arrow'); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Arrow\Parquet\RowsWriter;
use Flow\ETL\Adapter\Parquet\{NativeParquetWriter, SchemaConverter};
use Flow\Parquet\Engine\Arrow\{OptionsConverter, SchemaConverter as ArrowSchemaConverter};
use Flow\Parquet\Options;
use Flow\Types\Value\Uuid;

use function Flow\ETL\DSL\{bool_schema, date_schema, datetime_schema, float_schema, int_schema, json_schema, list_schema, map_schema, schema, str_schema, structure_schema, time_schema, uuid_schema};
use function Flow\Filesystem\DSL\{memory_filesystem, path};
use function Flow\Types\DSL\{type_integer, type_list, type_map, type_optional, type_string, type_structure};

$schema = schema(
    int_schema('int', nullable: true),
    float_schema('float', nullable: true),
    bool_schema('bool', nullable: true),
    str_schema('string', nullable: true),
    datetime_schema('datetime', nullable: true),
    date_schema('date', nullable: true),
    time_schema('time', nullable: true),
    uuid_schema('uuid', nullable: true),
    json_schema('json', nullable: true),
    list_schema('list', type_list(type_optional(type_integer())), nullable: true),
    map_schema('map', type_map(type_string(), type_optional(type_integer())), nullable: true),
    structure_schema('structure', type_structure(['id' => type_integer(), 'name' => type_optional(type_string())]), nullable: true),
    list_schema('list_of_structures', type_list(type_structure(['id' => type_integer(), 'tags' => type_optional(type_list(type_string()))])), nullable: true),
    map_schema('map_of_lists', type_map(type_string(), type_list(type_integer())), nullable: true),
);
$values = [
    [
        'int' => -7, 'float' => 0.14, 'bool' => true, 'string' => 'zażółć',
        'datetime' => new DateTimeImmutable('1969-12-31 23:59:59.999999 UTC'), 'date' => new DateTimeImmutable('1900-01-01'),
        'time' => new DateInterval('PT1H2M3S'), 'uuid' => new Uuid('26fd21b0-6080-4d6c-bdb4-1214f1feffef'), 'json' => '{"a":[1,2]}',
        'list' => [1, null, 3], 'map' => ['a' => 1, 'b' => null], 'structure' => ['id' => 1, 'name' => null],
        'list_of_structures' => [['id' => 1, 'tags' => ['x', 'y']], ['id' => 2, 'tags' => null]],
        'map_of_lists' => ['a' => [1, 2], 'b' => []],
    ],
    array_fill_keys(array_keys($schema->definitions()), null),
];

$filesystem = memory_filesystem();
$writer = new NativeParquetWriter(new RowsWriter(
    $filesystem->writeTo(path('memory://layout.parquet')),
    ArrowSchemaConverter::toExtension((new SchemaConverter())->toParquet($schema)),
    'SNAPPY',
    OptionsConverter::toExtension(Options::default()),
    1_000,
));
$writer->write(php_rows($schema, $values));
$writer->close();

$read = [];
$reader = native_parquet_reader($filesystem->readFrom(path('memory://layout.parquet')), $schema, 10, null, null);

while (($batch = $reader->next()) !== null) {
    array_push($read, ...$batch->toArray());
}

$written = php_rows($schema, $values)->toArray();

foreach (array_keys($schema->definitions()) as $name) {
    echo $name, ': ', comparable(array_column($read, $name)) === comparable(array_column($written, $name)) ? 'identical' : 'DIFFER', "\n";
}
?>
--EXPECT--
int: identical
float: identical
bool: identical
string: identical
datetime: identical
date: identical
time: identical
uuid: identical
json: identical
list: identical
map: identical
structure: identical
list_of_structures: identical
map_of_lists: identical
