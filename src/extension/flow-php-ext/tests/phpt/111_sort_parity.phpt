--TEST--
RustBackend::sort() orders rows exactly as PhpBackend::sort() for every lane, direction and null placement, keeps ties in input order (long runs, at the edges, under mixed directions), and hands kinds it does not order to PhpBackend
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Values\Column\{PhpBackend, RustBackend};
use Flow\ETL\Values\NullsOrder;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Sort\SortColumn;
use Flow\ETL\Values\SortOrder;
use Flow\Types\Value\Uuid;

use function Flow\ETL\DSL\{array_to_rows, bool_schema, date_schema, datetime_schema, enum_schema, float_schema, int_schema, json_schema, list_schema, schema, str_schema, time_schema, uuid_schema};
use function Flow\Types\DSL\{type_integer, type_list};

function sorted_sequence(Flow\ETL\Values\Column\Backend $backend, array $data, array $definitions, array $columns): string
{
    try {
        $rows = $backend->sort(array_to_rows($data, schema(int_schema('seq'), ...$definitions), $backend), $columns);

        return implode(',', $rows->reduceToArray('seq')) . ' ' . get_class($rows->column('seq'));
    } catch (Throwable $e) {
        return get_class($e) . ': ' . $e->getMessage();
    }
}

function rows_of(array $values): array
{
    return array_map(static fn(int $seq, mixed $value): array => ['seq' => $seq, 'k' => $value], array_keys($values), $values);
}

$utc = new DateTimeZone('UTC');
$lanes = [
    'integer' => [int_schema('k', nullable: true), [3, PHP_INT_MIN, null, 3, PHP_INT_MAX, -1, null, 0, 3]],
    'float' => [float_schema('k', nullable: true), [1.5, NAN, -0.0, null, 0.0, -INF, -NAN, INF, 1.5, null, -0.0, NAN]],
    'boolean' => [bool_schema('k', nullable: true), [true, false, null, true, false, null, true]],
    'string' => [str_schema('k', nullable: true), ['b', 'B', '', 'ą', "a\0", null, 'a', 'b', '', 'aa']],
    'json' => [json_schema('k', nullable: true), ['{"a":2}', '[]', null, '{"a":1}', '[]']],
    'enum' => [enum_schema('k', BackedStringEnum::class, nullable: true), [BackedStringEnum::two, BackedStringEnum::one, null, BackedStringEnum::three, BackedStringEnum::one]],
    'uuid' => [uuid_schema('k', nullable: true), [new Uuid('ffffffff-ffff-4fff-bfff-ffffffffffff'), new Uuid('00000000-0000-4000-8000-000000000000'), null, new Uuid('7fffffff-ffff-4fff-bfff-ffffffffffff'), new Uuid('00000000-0000-4000-8000-000000000000')]],
    'datetime' => [datetime_schema('k', nullable: true), [new DateTimeImmutable('2024-01-01 01:00:00', new DateTimeZone('+01:00')), new DateTimeImmutable('2024-01-01 00:00:00', $utc), null, new DateTimeImmutable('2023-12-31 23:59:59.999999', $utc), new DateTimeImmutable('2024-01-01 00:00:00', new DateTimeZone('America/New_York'))]],
    'date' => [date_schema('k', nullable: true), [new DateTimeImmutable('2024-01-01', $utc), new DateTimeImmutable('1970-01-01', $utc), null, new DateTimeImmutable('2024-01-01', $utc), new DateTimeImmutable('1969-12-31', $utc)]],
    'time' => [time_schema('k', nullable: true), [new DateInterval('PT1H'), new DateInterval('PT0S'), null, new DateInterval('PT59M60S'), new DateInterval('PT1M')]],
    'list (PhpBackend lane)' => [list_schema('k', type_list(type_integer()), nullable: true), [[2], [1, 5], null, [1], [2]]],
];

foreach ($lanes as $label => [$definition, $values]) {
    $same = true;
    $ascending = '';

    foreach ([SortOrder::ASC, SortOrder::DESC] as $order) {
        foreach ([NullsOrder::FIRST, NullsOrder::LAST] as $nulls) {
            $columns = [new SortColumn('k', $order, $nulls)];
            $php = sorted_sequence(new PhpBackend(), rows_of($values), [$definition], $columns);
            $rust = sorted_sequence(new RustBackend(), rows_of($values), [$definition], $columns);
            $ascending = $ascending === '' ? explode(' ', $rust)[0] : $ascending;

            if (explode(' ', $php)[0] !== explode(' ', $rust)[0]) {
                $same = false;
                echo $label, ' ', $order->name, ' ', $nulls->name, " MISMATCH\n  php:  ", $php, "\n  rust: ", $rust, "\n";
            }
        }
    }

    echo $label, ': ', $same ? 'same, asc nulls first ' . $ascending : 'MISMATCH', "\n";
}

$groups = [];
for ($i = 0; $i < 300; $i++) {
    $groups[] = ['seq' => $i, 'g' => ['x', 'y', 'z'][$i % 3 === 0 ? 0 : ($i < 150 ? 1 : 2)], 's' => $i % 4 === 0 ? null : (float) ($i % 5), 'n' => $i % 2];
}
$cases = [
    'one key, long tie runs ascending' => [[str_schema('g')], [new SortColumn('g')], $groups],
    'one key, long tie runs descending' => [[str_schema('g')], [new SortColumn('g', SortOrder::DESC)], $groups],
    'every row equal' => [[int_schema('n')], [new SortColumn('n', SortOrder::DESC)], array_map(static fn(array $row): array => ['seq' => $row['seq'], 'n' => 1], $groups)],
    'ties at the first and the last row' => [[int_schema('n')], [new SortColumn('n')], [['seq' => 0, 'n' => 5], ['seq' => 1, 'n' => 5], ['seq' => 2, 'n' => 3], ['seq' => 3, 'n' => 7], ['seq' => 4, 'n' => 7]]],
    'two keys, mixed directions' => [[str_schema('g'), float_schema('s', nullable: true)], [new SortColumn('g'), new SortColumn('s', SortOrder::DESC)], $groups],
    'three keys, mixed directions and nulls' => [[str_schema('g'), float_schema('s', nullable: true), int_schema('n')], [new SortColumn('g', SortOrder::DESC), new SortColumn('s', SortOrder::ASC, NullsOrder::LAST), new SortColumn('n', SortOrder::DESC, NullsOrder::FIRST)], $groups],
    'no rows' => [[int_schema('n')], [new SortColumn('n')], []],
    'one row' => [[int_schema('n')], [new SortColumn('n')], [['seq' => 0, 'n' => 1]]],
    'no sort column' => [[int_schema('n')], [], [['seq' => 0, 'n' => 2], ['seq' => 1, 'n' => 1]]],
];

foreach ($cases as $label => [$definitions, $columns, $data]) {
    $names = ['seq', ...array_map(static fn(Definition $definition): string => $definition->name(), $definitions)];
    $data = array_map(static fn(array $row): array => array_intersect_key($row, array_flip($names)), $data);
    $php = sorted_sequence(new PhpBackend(), $data, $definitions, $columns);
    $rust = sorted_sequence(new RustBackend(), $data, $definitions, $columns);
    $order = explode(' ', $rust)[0];

    echo $label, ': ', explode(' ', $php)[0] === $order ? 'same ' . (strlen($order) > 50 ? substr($order, 0, 50) . '...' : $order) : "MISMATCH\n  php:  " . $php . "\n  rust: " . $rust, "\n";
}

$numericName = static function (Flow\ETL\Values\Column\Backend $backend): string {
    try {
        return json_encode($backend->sort(array_to_rows([['a' => 2, '1' => 20], ['a' => 1, '1' => 10]], schema(int_schema('a'), int_schema('1')), $backend), [new SortColumn('a')])->toArray());
    } catch (Throwable $e) {
        return get_class($e) . ': ' . $e->getMessage();
    }
};
echo 'a numeric column name: ', $numericName(new PhpBackend()) === $numericName(new RustBackend()) ? 'same ' : 'differs ', $numericName(new RustBackend()), "\n";
echo get_class((new RustBackend())->sort(array_to_rows(rows_of([2, 1]), schema(int_schema('seq'), int_schema('k')), new RustBackend()), [new SortColumn('k')])->column('k')), "\n";
?>
--EXPECT--
integer: same, asc nulls first 2,6,1,5,7,0,3,8,4
float: same, asc nulls first 3,9,5,2,4,10,0,8,7,1,6,11
boolean: same, asc nulls first 2,5,1,4,0,3,6
string: same, asc nulls first 5,2,8,1,6,4,9,0,7,3
json: same, asc nulls first 2,1,4,3,0
enum: same, asc nulls first 2,1,4,3,0
uuid: same, asc nulls first 2,1,4,3,0
datetime: same, asc nulls first 2,3,0,1,4
date: same, asc nulls first 2,4,1,0,3
time: same, asc nulls first 2,1,4,0,3
list (PhpBackend lane): same, asc nulls first 2,3,0,4,1
one key, long tie runs ascending: same 0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51,...
one key, long tie runs descending: same 151,152,154,155,157,158,160,161,163,164,166,167,16...
every row equal: same 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,...
ties at the first and the last row: same 2,0,1,3,4
two keys, mixed directions: same 0,12,24,36,48,60,72,84,96,108,120,132,144,156,168,...
three keys, mixed directions and nulls: same 155,175,185,205,215,235,245,265,275,295,170,190,23...
no rows: same 
one row: same 0
no sort column: same 0,1
a numeric column name: same [{"a":1,"1":10},{"a":2,"1":20}]
Flow\ETL\Values\Column\RustColumn
