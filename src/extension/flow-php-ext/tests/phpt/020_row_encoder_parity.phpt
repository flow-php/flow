--TEST--
RustFloeEncoderNative produces ROW frame bodies byte-identical to PhpFloeEncoder
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Rows;
use Flow\Filesystem\Partition;
use Flow\Floe\PhpFloeEncoder;
use Flow\Floe\RustFloeEncoderNative;

use function Flow\ETL\DSL\{row, rows, int_entry, str_entry, float_entry, bool_entry, datetime_entry, time_entry, uuid_entry, list_entry, map_entry, structure_entry, xml_entry, json_entry, null_entry, enum_entry, date_entry, schema_from_json};
use function Flow\Types\DSL\{type_list, type_map, type_structure, structure_element, type_integer, type_string, type_float, type_mixed, type_optional};

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
        map_entry('m', ['cpu' => 1.5], type_map(type_string(), type_float())),
        map_entry('mi', [7 => 'a', -1 => 'b'], type_map(type_integer(), type_string())),
        structure_entry('st', ['a' => 1], type_structure(['a' => type_integer(), 'b' => structure_element('b', type_string(), optional: true)])),
        structure_entry('st_interleaved', ['z' => 1, 'b' => 'x'], type_structure(['z' => type_integer(), 'a' => structure_element('a', type_string(), optional: true), 'b' => type_string()])),
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

foreach ($datasets as $label => $data) {
    printf("%-14s frames-identical:%s\n", $label, php_frames($data) === ext_frames($data) ? 'yes' : 'NO');
}

// A row narrower than the primed plan must emit VALUE_ABSENT (0x03) for the
// missing column, byte-identical to PhpFloeEncoder.
$wideBody = json_encode(row(int_entry('a', 1), str_entry('b', 'x'), float_entry('c', 1.5))->schema()->normalize(), JSON_THROW_ON_ERROR);
$narrowRow = row(int_entry('a', 7), float_entry('c', 2.5));

$hydrator = new PhpRowHydrator();
$narrowTyped = $hydrator->dehydrate(new Rows($narrowRow));

$php = new PhpFloeEncoder(schema_from_json($wideBody));
$ext = new RustFloeEncoderNative();
printf("%-14s frames-identical:%s\n", 'absent', $php->encode($narrowTyped) === $ext->encode($narrowTyped, $wideBody) ? 'yes' : 'NO');

$badRow = row(list_entry('bad', [1], type_list(type_mixed())));
$badExt = new RustFloeEncoderNative();
expect_exception(fn() => $badExt->encode($hydrator->dehydrate(new Rows($badRow)), json_encode($badRow->schema()->normalize(), JSON_THROW_ON_ERROR)));
?>
--EXPECT--
scalar         frames-identical:yes
from_null      frames-identical:yes
datetime       frames-identical:yes
containers     frames-identical:yes
enum_json_xml  frames-identical:yes
heterogeneous  frames-identical:yes
partitioned    frames-identical:yes
empty          frames-identical:yes
absent         frames-identical:yes
Flow\Floe\Exception\ExtensionException: flow_php does not support values of type "mixed" in this build
