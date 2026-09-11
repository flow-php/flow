--TEST--
NativeRowHydrator hydrate/dehydrate are serialize-identical to PhpRowHydrator
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
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

use Flow\ETL\Row\NativeRowHydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Row\RustRowHydratorNative;
use Flow\ETL\Schema\Metadata;

enum PhptSuit: string
{
    case Hearts = 'h';
}

$datasets = [
    'scalars' => [
        schema(int_schema('id'), str_schema('name', nullable: true), float_schema('p'), bool_schema('a')),
        [
            new RawRowValues(['id' => 1, 'name' => 'x', 'p' => 1.5, 'a' => true]),
            new RawRowValues(['id' => 2, 'name' => null, 'p' => -0.25, 'a' => false]),
        ],
    ],
    'null_nonnullable' => [
        schema(int_schema('id'), str_schema('name', nullable: true)),
        [new RawRowValues(['id' => 1, 'name' => null]), new RawRowValues(['id' => 2, 'name' => null])],
    ],
    'absent' => [
        schema(int_schema('id'), str_schema('name', nullable: true)),
        [new RawRowValues(['id' => 1]), new RawRowValues(['id' => 2, 'name' => 'y'])],
    ],
    'metadata' => [
        schema(int_schema('id')),
        [
            new RawRowValues(['id' => 1], ['id' => Metadata::fromArray(['k' => 'v1'])]),
            new RawRowValues(['id' => 2], ['id' => Metadata::fromArray(['k' => 'v2'])]),
        ],
    ],
    'temporal' => [
        schema(datetime_schema('at'), date_schema('d'), time_schema('t'), uuid_schema('u')),
        [new RawRowValues([
            'at' => new DateTimeImmutable('2025-01-01 12:00:00.123456', new DateTimeZone('Europe/Warsaw')),
            'd' => new DateTimeImmutable('2025-03-01'),
            't' => new DateInterval('PT2H30M5S'),
            'u' => new Flow\Types\Value\Uuid('01234567-89ab-4def-8123-456789abcdef'),
        ])],
    ],
    'containers' => [
        schema(
            list_schema('l', type_list(type_integer())),
            map_schema('m', type_map(type_string(), type_integer())),
            structure_schema('st', type_structure(['a' => type_integer(), 'b' => structure_element('b', type_string(), optional: true)], true)),
            list_schema('opt', type_list(type_optional(type_integer()))),
            json_schema('j'),
        ),
        [new RawRowValues([
            'l' => [1, 2, 3],
            'm' => ['a' => 1],
            'st' => ['a' => 1, 'extra' => 'kept'],
            'opt' => [1, null],
            'j' => Flow\Types\Value\Json::fromArray(['x' => 1]),
        ])],
    ],
    'enum_and_null' => [
        schema(enum_schema('s', PhptSuit::class, nullable: true), null_schema('n')),
        [new RawRowValues(['s' => PhptSuit::Hearts, 'n' => null]), new RawRowValues(['s' => null, 'n' => null])],
    ],
    'empty' => [schema(int_schema('id')), []],
];

$php = new PhpRowHydrator();
$native = new NativeRowHydrator();

foreach ($datasets as $label => [$s, $batch]) {
    $hydrateOk = serialize($php->hydrate($batch, $s)) === serialize($native->hydrate($batch, $s));
    $rows = $php->hydrate($batch, $s);
    $dehydrateOk = serialize($php->dehydrate($rows)) === serialize($native->dehydrate($rows));
    printf("%-16s hydrate:%s dehydrate:%s\n", $label, $hydrateOk ? 'yes' : 'NO', $dehydrateOk ? 'yes' : 'NO');
}

$mutated = schema(int_schema('id'));
$php->hydrate([new RawRowValues(['id' => 1])], $mutated);
$native->hydrate([new RawRowValues(['id' => 1])], $mutated);
$mutated->add(str_schema('name', nullable: true))->makeNullable();
// Schema is immutable, so $mutated never gained "name" - the column is simply not hydrated
$batch = [new RawRowValues(['id' => 1, 'name' => 'x'])];
printf(
    "%-16s hydrate:%s\n",
    'schema_mutation',
    serialize($php->hydrate($batch, $mutated)) === serialize($native->hydrate($batch, $mutated)) ? 'yes' : 'NO',
);

printf("native class registered:%s\n", class_exists(RustRowHydratorNative::class, false) ? 'yes' : 'NO');
?>
--EXPECT--
scalars          hydrate:yes dehydrate:yes
null_nonnullable hydrate:yes dehydrate:yes
absent           hydrate:yes dehydrate:yes
metadata         hydrate:yes dehydrate:yes
temporal         hydrate:yes dehydrate:yes
containers       hydrate:yes dehydrate:yes
enum_and_null    hydrate:yes dehydrate:yes
empty            hydrate:yes dehydrate:yes
schema_mutation  hydrate:yes
native class registered:yes
