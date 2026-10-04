--TEST--
RustColumnBuilder casts and refuses exactly as the PHP builder through append() and appendMany(), in every default zone
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Column\RustBackend;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Tests\Double\ForeignTypeDefinition;
use Flow\ETL\Tests\Double\ThrowingType;

use function Flow\ETL\DSL\{bool_schema, date_schema, datetime_schema, float_schema, int_schema, json_schema, list_schema, map_schema, str_schema, uuid_schema};
use function Flow\Types\DSL\{type_integer, type_list, type_map, type_positive_integer, type_string};

$datetimes = [
    '2026-01-02T03:04:05Z', '2026-01-02 03:04:05Z', '2026-01-02T03:04Z', "2026-01-02T03:04:05Z\n", '2026-01-02T03:04:05.1Z',
    '2026-01-02T03:04:05.123456Z', '2026-01-02T03:04:05.1234567Z', '2026-01-02T03:04:05.123456789Z', '2026-12-31T23:59:60Z',
    '2026-01-02T24:00:00Z', '0001-01-01T00:00:00Z', '2026-01-02T03:04:05.123456789+02:00', '2026-01-02T03:04:05-0530',
    '2026-01-02T03:04:05-05', '2026-01-02T03:04:05+00:00', '2026-01-02 03:04', '2026-01-02T03:04:05', "2026-01-02T03:04\n",
    '2026-03-29T02:30:00', '2026-10-25T02:30:00', '2026-03-08T02:30:00', '2026-11-01T01:30:00',
    '2026-01-02T25:99:99Z', '2026-01-02T03:60:00Z', '2026-01-02T03:04:05+25:00',
    '1969-12-31T23:59:59.5Z', '2024-02-29T12:00:00+14:00', '2024-02-29T12:00:00-12:00', '2026-01-31T00:00:00+23:59',
    '9999-12-31T23:59:59.999999-23:59', '0001-01-01T00:00:00+23:59', '2038-01-19T03:14:08Z',
    '2026-01-02T03:04:05-00:00', '2026-01-02t03:04:05Z', '2026-01-02T03:04:05z', '2026-01-02T03:04:05,5Z',
    '2026-01-02T03:04:05+0100', '2026-01-02T03:04:05.Z', '2023-02-29T00:00:00Z', '0000-01-01T00:00:00Z',
    '2026-01-02T03:04:05+24:00', '2026-01-02T03:04:05+01:60', '2026-01-02T03:04:05.1234567+01:00',
];
$dates = ['2026-01-02', '2024-02-29', "2026-01-02\n", '0001-01-01', '9999-12-31', '2026-09-06'];
$cases = [];

foreach (['UTC', 'Europe/Warsaw', 'America/New_York', '+05:30'] as $zone) {
    foreach ($datetimes as $value) {
        $cases[] = [datetime_schema('at', zone: $zone), [$value]];
    }
}

foreach ($dates as $value) {
    $cases[] = [date_schema('on'), [$value]];
    $cases[] = [datetime_schema('at'), [$value]];
    $cases[] = [datetime_schema('at_scl', zone: 'America/Santiago'), [$value]];
}

foreach ([
    [int_schema('id'), ['abc']], [float_schema('p'), ['0x1A']], [bool_schema('a'), ['weird']], [int_schema('id'), [[1, 2, 3]]],
    [uuid_schema('u'), ['not-a-uuid']], [uuid_schema('u'), ['01234567-89AB-4DEF-8123-456789ABCDEF']], [json_schema('j'), [5]],
    [json_schema('j'), ['{oops']], [json_schema('j'), ['plain']], [datetime_schema('at'), ['not-a-date']],
    [datetime_schema('at'), [['nope']]], [date_schema('d'), ['not-a-date']],
    [map_schema('m', type_map(type_string(), type_integer())), [[5 => 1]]],
    [list_schema('l', type_list(type_integer())), [[1 => 'x']]], [list_schema('l', type_list(type_positive_integer())), [['abc']]],
    [list_schema('l', type_list(type_positive_integer())), [[-3]]], [int_schema('i'), ['1', 'x']],
    [int_schema('id'), ['9223372036854775808']], [date_schema('d'), ['']], [datetime_schema('at'), ['now']],
    [list_schema('l', type_list(type_string())), [['a', null]]], [list_schema('l', type_list(type_integer())), [5]],
    [new ForeignTypeDefinition('a', new ThrowingType(new LogicException('stub type refuses everything'))), [[1, 2]]],
    [datetime_schema('at'), ['+292278994-08-17T07:12:55Z']],
    [int_schema('id'), ["\f5"]], [int_schema('id'), ["5\f"]], [float_schema('p'), ['-0']],
    [float_schema('p'), ['NAN']], [float_schema('p'), ['INF']], [float_schema('p'), ['-INF']],
    [float_schema('p'), ['nan']], [float_schema('p'), ['Inf']], [float_schema('p'), ['+INF']],
] as $case) {
    $cases[] = $case;
}

foreach ([bool_schema('a'), str_schema('s'), date_schema('d'), datetime_schema('at')] as $definition) {
    foreach ([NAN, INF, -INF] as $value) {
        $cases[] = [$definition, [$value]];
    }
}

$build = static fn(object $backend, object $definition, array $values, bool $many): Closure => static function () use ($backend, $definition, $values, $many): array {
    $builder = $backend->builder($definition);

    if ($many) {
        $builder->appendMany($values);
    } else {
        foreach ($values as $value) {
            $builder->append($value);
        }
    }

    return $builder->finish()->values();
};

foreach (['UTC', 'Europe/Warsaw', 'America/Santiago'] as $timezone) {
    ini_set('date.timezone', $timezone);
    $identical = 0;

    foreach ($cases as [$definition, $values]) {
        foreach ([false, true] as $many) {
            $php = outcome($build(new PhpBackend(), $definition, $values, $many));
            $native = outcome($build(new RustBackend(), $definition, $values, $many));

            if ($php === $native) {
                $identical++;
            } else {
                echo ($many ? 'appendMany ' : 'append ') . $definition->type()->toString() . ' ' . json_encode($values) . "\n  php:    {$php}\n  native: {$native}\n";
            }
        }
    }

    echo "date.timezone {$timezone}: {$identical} of " . (2 * count($cases)) . " identical\n";
}
?>
--EXPECT--
date.timezone UTC: 470 of 470 identical
date.timezone Europe/Warsaw: 470 of 470 identical
date.timezone America/Santiago: 470 of 470 identical
