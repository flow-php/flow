--TEST--
NativeRowHydrator cast is serialize-identical to PhpRowHydrator
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
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_non_empty_string;
use function Flow\Types\DSL\type_numeric_string;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_positive_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

use Flow\ETL\Row\NativeRowHydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Row\RustRowHydratorNative;
use Flow\ETL\Schema\Metadata;

enum CastSuit: string
{
    case Hearts = 'h';
}

$shared = new DateTimeImmutable('2025-01-01 12:00:00.123456', new DateTimeZone('Europe/Warsaw'));

$datasets = [
    'scalars' => [
        schema(int_schema('id'), float_schema('p'), bool_schema('a'), str_schema('n', nullable: true)),
        [
            new RawRowValues(['id' => '42', 'p' => '3.14', 'a' => 'yes', 'n' => 7]),
            new RawRowValues(['id' => ' 7', 'p' => '1e3', 'a' => 'OFF', 'n' => 1.5]),
            new RawRowValues(['id' => '007', 'p' => '.5', 'a' => 'ON', 'n' => true]),
            new RawRowValues(['id' => '9223372036854775807', 'p' => true, 'a' => 3.5, 'n' => null]),
            new RawRowValues(['id' => 5, 'p' => 2.5, 'a' => true, 'n' => 'text']),
        ],
    ],
    'nested_families' => [
        schema(
            list_schema('counts', type_list(type_positive_integer())),
            structure_schema('labels', type_structure(['label' => type_non_empty_string()])),
            structure_schema('codes', type_structure(['code' => type_numeric_string()])),
        ),
        [
            new RawRowValues(['counts' => [5, 7], 'labels' => ['label' => 'x'], 'codes' => ['code' => '42']]),
            new RawRowValues(['counts' => ['5', 8], 'labels' => ['label' => 9], 'codes' => ['code' => 42]]),
        ],
    ],
    'temporal' => [
        schema(datetime_schema('at'), datetime_schema('at2'), date_schema('d')),
        [
            new RawRowValues(['at' => $shared, 'at2' => $shared, 'd' => $shared]),
            new RawRowValues(['at' => $shared, 'at2' => '2024-03-01 10:20:30', 'd' => new DateTimeImmutable('2025-03-01')]),
            new RawRowValues(['at' => 1700000000, 'at2' => 1700000000.5, 'd' => '2024-03-05 08:30:00']),
        ],
    ],
    'uuid_json' => [
        schema(uuid_schema('u', nullable: true), json_schema('j')),
        [
            new RawRowValues(['u' => '01234567-89ab-4def-8123-456789abcdef', 'j' => '["a","b"]']),
            new RawRowValues(['u' => new Flow\Types\Value\Uuid('01234567-89ab-4def-8123-456789abcdef'), 'j' => '{"a":1}']),
            new RawRowValues(['u' => null, 'j' => ['a' => 1]]),
            new RawRowValues(['u' => '11234567-89ab-4def-8123-456789abcdef', 'j' => Flow\Types\Value\Json::fromArray(['x' => 1])]),
        ],
    ],
    'containers' => [
        schema(
            list_schema('l', type_list(type_integer())),
            map_schema('m', type_map(type_string(), type_integer())),
            map_schema('mi', type_map(type_integer(), type_string())),
            list_schema('lo', type_list(type_optional(type_integer()))),
            list_schema('lf', type_list(type_float())),
            structure_schema('st', type_structure(['a' => type_integer(), 'b' => structure_element('b', type_string(), optional: true)], true)),
        ),
        [
            new RawRowValues([
                'l' => ['1', 2, '3'],
                'm' => ['a' => '1', 'b' => 2],
                'mi' => [0 => 'x', 5 => 7],
                'lo' => ['1', null, 3],
                'lf' => [33, 65.5],
                'st' => ['a' => '5', 'extra' => 'dropped'],
            ]),
            new RawRowValues(['l' => [], 'm' => [], 'mi' => [], 'lo' => [], 'lf' => [], 'st' => ['a' => 1, 'b' => 'kept']]),
        ],
    ],
    'exotic_fallback' => [
        schema(enum_schema('s', CastSuit::class), xml_schema('x'), time_schema('t')),
        [
            new RawRowValues(['s' => CastSuit::Hearts, 'x' => '<root a="1"><i>v</i></root>', 't' => new DateInterval('PT2H30M5S')]),
            new RawRowValues(['s' => 'h', 'x' => '<other/>', 't' => 'PT2H']),
        ],
    ],
    'fill_and_metadata' => [
        schema(int_schema('id', nullable: true), str_schema('name', nullable: true), bool_schema('flag', nullable: true)),
        [
            new RawRowValues(['id' => '1'], ['id' => Metadata::fromArray(['k' => 'v1'])]),
            new RawRowValues([]),
            new RawRowValues(['id' => 2, 'name' => null, 'unknown' => 'dropped', 'flag' => 'on']),
            new RawRowValues(['id' => null], ['id' => Metadata::fromArray(['k' => 'v2'])]),
        ],
    ],
    'all_optional_st' => [
        schema(structure_schema('st', type_structure(['b' => structure_element('b', type_string(), optional: true)]))),
        [new RawRowValues(['st' => ['other' => 1]])],
    ],
    'interleaved_st' => [
        schema(structure_schema('st', type_structure(['z' => type_integer(), 'a' => structure_element('a', type_string(), optional: true), 'b' => type_string()]))),
        [
            new RawRowValues(['st' => ['b' => 'x', 'z' => '5']]),
            new RawRowValues(['st' => ['z' => 1, 'a' => 'present', 'b' => 'y']]),
        ],
    ],
    'empty' => [schema(int_schema('id')), []],
    // every column present in every row: the batch skips Row::conform(), which would return each row unchanged
    'all_present' => [
        schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('a'), float_schema('p', nullable: true)),
        [
            new RawRowValues(['id' => '1', 'name' => 'x', 'a' => 'yes', 'p' => '1.5']),
            new RawRowValues(['id' => '2', 'name' => null, 'a' => 'no', 'p' => null]),
            new RawRowValues(['p' => 3, 'a' => true, 'name' => 'z', 'id' => 3]),
        ],
    ],
    'absent_nullable' => [
        schema(int_schema('id'), str_schema('name', nullable: true)),
        [
            new RawRowValues(['id' => 1, 'name' => 'a']),
            new RawRowValues(['id' => 2, 'name' => 'b']),
            new RawRowValues(['id' => 3, 'name' => 'c']),
            new RawRowValues(['id' => 4]),
            new RawRowValues(['id' => 5, 'name' => 'e']),
        ],
    ],
    'numeric_names' => [
        schema(int_schema('id'), str_schema('7'), int_schema('10', nullable: true)),
        [
            new RawRowValues(['id' => '1', '7' => 'seven', '10' => '10']),
            new RawRowValues(['10' => null, '7' => 'eight', 'id' => 2]),
        ],
    ],
];

$php = new PhpRowHydrator();
$native = new NativeRowHydrator();

foreach ($datasets as $label => [$s, $batch]) {
    $hydrateOk = serialize($php->hydrate($batch, $s)) === serialize($native->hydrate($batch, $s));
    printf("%-16s hydrate:%s\n", $label, $hydrateOk ? 'yes' : 'NO');
}

// TypeDetector types [33, 65.5] as list<float>; both hydrators must materialize it as floats.
$promotion = schema(list_schema('lf', type_list(type_float())));
$promotionBatch = [new RawRowValues(['lf' => [33, 65.5]])];
printf(
    "%-16s php:%s native:%s\n",
    'list_promotion',
    json_encode($php->hydrate($promotionBatch, $promotion)->first()->get('lf'), JSON_PRESERVE_ZERO_FRACTION),
    json_encode($native->hydrate($promotionBatch, $promotion)->first()->get('lf'), JSON_PRESERVE_ZERO_FRACTION),
);

$mutated = schema(int_schema('id'));
$php->hydrate([new RawRowValues(['id' => '1'])], $mutated);
$native->hydrate([new RawRowValues(['id' => '1'])], $mutated);
$mutated->add(str_schema('name', nullable: true))->makeNullable();
// Schema is immutable, so $mutated never gained "name" - the column is simply not cast
$batch = [new RawRowValues(['id' => '1', 'name' => 7])];
printf(
    "%-16s hydrate:%s\n",
    'schema_mutation',
    serialize($php->hydrate($batch, $mutated)) === serialize($native->hydrate($batch, $mutated)) ? 'yes' : 'NO',
);

// row 3 lacks a NOT NULL column and row 5 is refused by its cast: the cast refusal is raised while the batch is
// built, before Rows::conformed() could report the absence, so both paths name row 5
$refused = schema(int_schema('id'), int_schema('n'));
$refusedBatch = [
    new RawRowValues(['id' => 0, 'n' => 0]),
    new RawRowValues(['id' => 1, 'n' => 1]),
    new RawRowValues(['id' => 2, 'n' => 2]),
    new RawRowValues(['id' => 3]),
    new RawRowValues(['id' => 4, 'n' => 4]),
    new RawRowValues(['id' => 5, 'n' => 'five']),
];
$refusals = [];

foreach (['php' => $php, 'native' => $native] as $side => $hydrator) {
    try {
        $hydrator->hydrate($refusedBatch, $refused);
        $refusals[$side] = 'none';
    } catch (Throwable $e) {
        $refusals[$side] = $e::class . ': ' . $e->getMessage();
    }
}

printf("%-16s identical:%s\n", 'absent_then_refused', $refusals['php'] === $refusals['native'] ? 'yes' : 'NO');
echo $refusals['native'], "\n";

// a value held by reference is cast by value: the hydrated row must not change when the caller's variable does
$referenced = [
    'ref_json' => [schema(json_schema('c')), '{"a":1}', static fn(&$v): array => ['c' => &$v]],
    'ref_uuid' => [schema(uuid_schema('c')), '01234567-89ab-4def-8123-456789abcdef', static fn(&$v): array => ['c' => &$v]],
    'ref_null' => [schema(str_schema('c', nullable: true)), null, static fn(&$v): array => ['c' => &$v]],
    'ref_datetime' => [schema(datetime_schema('c')), new DateTimeImmutable('2020-01-01 00:00:00'), static fn(&$v): array => ['c' => &$v]],
    'ref_list_item' => [schema(list_schema('c', type_list(type_positive_integer()))), 5, static fn(&$v): array => ['c' => [&$v]]],
    'ref_st_element' => [
        schema(structure_schema('c', type_structure(['s' => type_non_empty_string()]))),
        'text',
        static fn(&$v): array => ['c' => ['s' => &$v]],
    ],
];

foreach ($referenced as $label => [$s, $value, $wrap]) {
    $hydrated = [];

    foreach (['php' => $php, 'native' => $native] as $side => $hydrator) {
        $v = $value;
        $rows = $hydrator->hydrate([new RawRowValues($wrap($v))], $s);
        $v = 'mutated';
        $hydrated[$side] = serialize($rows);
    }

    printf("%-16s hydrate:%s\n", $label, $hydrated['php'] === $hydrated['native'] ? 'yes' : 'NO');
}

$isoStrings = [
    '2026-07-13T10:20:30+00:00',
    "2026-07-13T10:20:30+00:00\n",
    "2026-07-13T10:20:30+00:00\n\n",
    '2026-07-13T10:20:30Z',
    '2026-07-13T10:20:30+05',
    '2026-07-13T10:20:30+0530',
    '2026-07-13T10:20:30+05:30',
    '2026-07-13T10:20:30-12:00',
    '2026-07-13 10:20:30',
    '2026-07-13T10:20',
    '2026-07-13T10:20:30.1Z',
    '2026-07-13T10:20:30.12Z',
    '2026-07-13T10:20:30.123Z',
    '2026-07-13T10:20:30.1234Z',
    '2026-07-13T10:20:30.12345Z',
    '2026-07-13T10:20:30.123456Z',
    '2026-07-13T10:20:30.1234567Z',
    '2026-07-13T10:20:30.12345678Z',
    '2026-07-13T10:20:30.123456789Z',
    '2026-07-13T10:20:30.1234567890Z',
    '2026-02-30T00:00:00Z',
    '2024-02-29T00:00:00Z',
    '2023-02-29T00:00:00Z',
    '0000-01-01T00:00:00Z',
    '2026-07-13T25:99:99Z',
    '２０２６-07-13T10:20:30Z',
    'now',
];

// the gate's only outcome-changing check is checkdate(): PHP throws on an impossible day, the constructor rolls it over
foreach (['0000', '0001', '1900', '2000', '2023', '2024', '2100'] as $year) {
    for ($month = 0; $month <= 13; $month++) {
        for ($day = 0; $day <= 32; $day++) {
            $isoStrings[] = sprintf('%s-%02d-%02dT00:00:00Z', $year, $month, $day);
        }
    }
}

$isoSchema = schema(datetime_schema('at'));

foreach (['UTC', 'Europe/Warsaw'] as $zone) {
    ini_set('date.timezone', $zone);
    $mismatches = 0;

    foreach ($isoStrings as $isoString) {
        $results = [];

        foreach (['php' => $php, 'native' => $native] as $side => $hydrator) {
            try {
                $at = $hydrator->hydrate([new RawRowValues(['at' => $isoString])], $isoSchema)->first()->get('at');
                $results[$side] = $at::class . ' ' . $at->format('Y-m-d\TH:i:s.uP') . ' ' . $at->getTimezone()->getName();
            } catch (Throwable $e) {
                $results[$side] = $e::class . ': ' . $e->getMessage();
            }
        }

        if ($results['php'] !== $results['native']) {
            $mismatches++;
            echo '  ', var_export($isoString, true), ": php[{$results['php']}] native[{$results['native']}]\n";
        }
    }

    printf("iso datetime %-13s %d strings, %d mismatches\n", $zone, count($isoStrings), $mismatches);
}

printf("native class registered:%s\n", class_exists(RustRowHydratorNative::class, false) ? 'yes' : 'NO');
?>
--EXPECT--
scalars          hydrate:yes
nested_families  hydrate:yes
temporal         hydrate:yes
uuid_json        hydrate:yes
containers       hydrate:yes
exotic_fallback  hydrate:yes
fill_and_metadata hydrate:yes
all_optional_st  hydrate:yes
interleaved_st   hydrate:yes
empty            hydrate:yes
all_present      hydrate:yes
absent_nullable  hydrate:yes
numeric_names    hydrate:yes
list_promotion   php:[33.0,65.5] native:[33.0,65.5]
schema_mutation  hydrate:yes
absent_then_refused identical:yes
Flow\ETL\Exception\SchemaMismatchException: Rows do not match their schema: column "n" (row 5): could not convert 'five' (string) to integer
ref_json         hydrate:yes
ref_uuid         hydrate:yes
ref_null         hydrate:yes
ref_datetime     hydrate:yes
ref_list_item    hydrate:yes
ref_st_element   hydrate:yes
iso datetime UTC           3261 strings, 0 mismatches
iso datetime Europe/Warsaw 3261 strings, 0 mismatches
native class registered:yes
