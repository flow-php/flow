--TEST--
RustFloeEncoderNative produces ROW frame bodies byte-identical to PhpFloeEncoder
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_from_json;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Rows;
use Flow\Filesystem\Partition;
use Flow\Floe\PhpFloeEncoder;
use Flow\Floe\RustFloeEncoderNative;

enum PhptColor: string
{
    case Red = 'red';
}

$datasets = [
    'scalar' => rows(schema(int_schema('id'), str_schema('name', nullable: true), str_schema('e'), float_schema('p'), bool_schema('a'), int_schema('s')), row(['id' => 1, 'name' => null, 'e' => "b\x00in", 'p' => -1.5, 'a' => true, 's' => PHP_INT_MIN]), row(['id' => 2, 'name' => 'x', 'e' => '', 'p' => 0.25, 'a' => false, 's' => PHP_INT_MAX])),
    'from_null' => rows(schema(int_schema('id'), null_schema('n')), row(['id' => 1, 'n' => null])),
    'datetime' => rows(schema(datetime_schema('at'), date_schema('d'), time_schema('t'), uuid_schema('u')), row(['at' => new DateTimeImmutable('2025-01-01 12:00:00.123456', new DateTimeZone('Europe/Warsaw')), 'd' => new DateTimeImmutable('2025-03-01'), 't' => new DateInterval('PT2H30M5S'), 'u' => type_uuid()->cast('01234567-89ab-4def-8123-456789abcdef')])),
    'containers' => rows(schema(list_schema('ints', type_list(type_integer())), map_schema('m', type_map(type_string(), type_float())), map_schema('mi', type_map(type_integer(), type_string())), structure_schema('st', type_structure(['a' => type_integer(), 'b' => structure_element('b', type_string(), optional: true)])), structure_schema('st_interleaved', type_structure(['z' => type_integer(), 'a' => structure_element('a', type_string(), optional: true), 'b' => type_string()])), list_schema('opt', type_list(type_optional(type_integer())))), row(['ints' => [1, 2, 3], 'm' => ['cpu' => 1.5], 'mi' => [7 => 'a', -1 => 'b'], 'st' => ['a' => 1], 'st_interleaved' => ['z' => 1, 'b' => 'x'], 'opt' => [1, null]])),
    'enum_json_xml' => rows(schema(enum_schema('en', PhptColor::class), json_schema('j'), xml_schema('x')), row(['en' => PhptColor::Red, 'j' => type_json()->cast(['a' => 1]), 'x' => type_xml()->cast('<root a="1"><i>v</i></root>')])),
    'heterogeneous' => rows(schema(int_schema('id'), str_schema('n')), row(['id' => 1]), row(['id' => 2, 'n' => 'x']), row(['n' => 'y', 'id' => 3])),
    'partitioned' => Rows::partitioned(schema(int_schema('id'), str_schema('g')), [row(['id' => 1, 'g' => 'a'])], [new Partition('g', 'a')]),
    'empty' => rows(schema()),
];

foreach ($datasets as $label => $data) {
    printf("%-14s frames-identical:%s\n", $label, php_frames($data) === ext_frames($data) ? 'yes' : 'NO');
}

// A row narrower than the primed plan must emit VALUE_ABSENT (0x03) for the
// missing column, byte-identical to PhpFloeEncoder.
$wideBody = json_encode(schema(int_schema('a'), str_schema('b'), float_schema('c'))->normalize(), JSON_THROW_ON_ERROR);

$hydrator = new PhpRowHydrator();
$narrowTyped = $hydrator->dehydrate(new Rows(schema(int_schema('a'), float_schema('c')), row(['a' => 7, 'c' => 2.5])));

$php = new PhpFloeEncoder(schema_from_json($wideBody));
$ext = new RustFloeEncoderNative();
printf("%-14s frames-identical:%s\n", 'absent', $php->encode($narrowTyped) === $ext->encode($narrowTyped, $wideBody) ? 'yes' : 'NO');

$badSchema = schema(list_schema('bad', type_list(type_mixed())));
$badTyped = $hydrator->dehydrate(new Rows($badSchema, row(['bad' => [new SplStack()]])));
$badExt = new RustFloeEncoderNative();
expect_exception(fn() => $badExt->encode($badTyped, json_encode($badSchema->normalize(), JSON_THROW_ON_ERROR)));
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
