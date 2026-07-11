--TEST--
RowsEncoder produces ROW frame bodies byte-identical to Flow\Floe\RowEncoder
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Rows;
use Flow\Filesystem\Partition;
use Flow\Floe\FloeWriter;
use Flow\Floe\RowEncoder;
use Flow\Floe\SchemaTracker;
use Flow\Floe\RowsEncoder;

use function Flow\ETL\DSL\{row, rows, int_entry, str_entry, float_entry, bool_entry, datetime_entry, time_entry, uuid_entry, list_entry, map_entry, structure_entry, xml_entry, json_entry, null_entry, enum_entry, date_entry};
use function Flow\Types\DSL\{type_list, type_map, type_structure, type_integer, type_string, type_float, type_mixed, type_optional};

enum PhptColor: string
{
    case Red = 'red';
}

$datasets = [
    'scalar' => rows(
        row(int_entry('id', 1), str_entry('name', null), str_entry('e', "b\x00in"), float_entry('p', -1.5), bool_entry('a', true), int_entry('s', PHP_INT_MIN)),
        row(int_entry('id', 2), str_entry('name', 'x'), str_entry('e', ''), float_entry('p', 0.25), bool_entry('a', false), int_entry('s', PHP_INT_MAX)),
    ),
    'from_null' => rows(row(int_entry('id', 1), null_entry('n'))),
    'datetime' => rows(row(
        datetime_entry('at', new DateTimeImmutable('2025-01-01 12:00:00.123456', new DateTimeZone('Europe/Warsaw'))),
        date_entry('d', new DateTimeImmutable('2025-03-01')),
        time_entry('t', new DateInterval('PT2H30M5S')),
        uuid_entry('u', '01234567-89ab-4def-8123-456789abcdef'),
    )),
    'containers' => rows(row(
        list_entry('ints', [1, 2, 3], type_list(type_integer())),
        list_entry('mixed', [1, 'x', null, ['a' => 1, 0 => 'z'], new DateTimeImmutable('2020-01-01', new DateTimeZone('UTC'))], type_list(type_mixed())),
        map_entry('m', ['cpu' => 1.5], type_map(type_string(), type_float())),
        map_entry('mi', [7 => 'a', -1 => 'b'], type_map(type_integer(), type_string())),
        structure_entry('st', ['a' => 1, 'extra' => 'kept'], type_structure(['a' => type_integer()], ['b' => type_string()], true)),
        list_entry('opt', [1, null], type_list(type_optional(type_integer()))),
    )),
    'enum_json_xml' => rows(row(
        enum_entry('en', PhptColor::Red),
        json_entry('j', ['a' => 1]),
        xml_entry('x', '<root a="1"><i>v</i></root>'),
    )),
    'heterogeneous' => rows(
        row(int_entry('id', 1)),
        row(int_entry('id', 2), str_entry('n', 'x')),
        row(str_entry('n', 'y'), int_entry('id', 3)),
    ),
    'partitioned' => Rows::partitioned([row(int_entry('id', 1), str_entry('g', 'a'))], [new Partition('g', 'a')]),
    'empty' => rows(),
];

$rowEncoder = new RowEncoder();
$tracker = new SchemaTracker();

foreach ($datasets as $label => $data) {
    $encoder = new RowsEncoder();
    $plan = null;
    $identical = true;

    foreach ($data->all() as $row) {
        if ($plan === null || !$tracker->fits($plan, $row)) {
            $plan = FloeWriter::growSectionPlan(null, $row);
            $encoder->schema($plan->schemaBody);
        }

        if ($encoder->row($row) !== $rowEncoder->encode($plan, $row)) {
            $identical = false;
        }
    }

    printf("%-14s bytes-identical:%s\n", $label, $identical ? 'yes' : 'NO');
}

// A row narrower than the primed plan must emit VALUE_ABSENT (0x03) for the
// missing column, byte-identical to the pure-PHP RowEncoder.
$widePlan = FloeWriter::growSectionPlan(null, row(int_entry('a', 1), str_entry('b', 'x'), float_entry('c', 1.5)));
$narrowRow = row(int_entry('a', 7), float_entry('c', 2.5));

$absentEncoder = new RowsEncoder();
$absentEncoder->schema($widePlan->schemaBody);
$absentIdentical = $absentEncoder->row($narrowRow) === $rowEncoder->encode($widePlan, $narrowRow);
printf("%-14s bytes-identical:%s\n", 'absent', $absentIdentical ? 'yes' : 'NO');

$badEncoder = new RowsEncoder();
$badRow = row(list_entry('bad', [new SplStack()], type_list(type_mixed())));
$badEncoder->schema(FloeWriter::growSectionPlan(null, $badRow)->schemaBody);
expect_exception(fn() => $badEncoder->row($badRow));
?>
--EXPECT--
scalar         bytes-identical:yes
from_null      bytes-identical:yes
datetime       bytes-identical:yes
containers     bytes-identical:yes
enum_json_xml  bytes-identical:yes
heterogeneous  bytes-identical:yes
partitioned    bytes-identical:yes
empty          bytes-identical:yes
absent         bytes-identical:yes
Flow\Floe\Exception\ExtensionException: flow_php does not support values of type "SplStack" in mixed/union context
