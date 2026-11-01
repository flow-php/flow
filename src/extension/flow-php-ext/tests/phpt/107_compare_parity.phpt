--TEST--
RustBackend::compare(), isNull() and isNotNull() answer exactly as PhpBackend for every operator over every pair of values, refusals included
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Column\RustBackend;
use Flow\ETL\Values\Schema\Definitions;
use Flow\Types\Type;
use Flow\Types\Type\Comparison\Operator;
use Flow\Types\Value\Uuid;

use function Flow\Types\DSL\{type_boolean, type_date, type_datetime, type_float, type_integer, type_json, type_numeric_string, type_string, type_time, type_uuid};

function column(Backend $backend, Type $type, array $values): Column
{
    $builder = $backend->builder(Definitions::of('c', $type, true));
    $builder->appendMany($values);

    return $builder->finish();
}

function compared(callable $answer): string
{
    try {
        $column = $answer();

        return $column->type()->toString() . ' ' . var_export($column->values(), true);
    } catch (Throwable $e) {
        return get_class($e) . ': ' . $e->getMessage();
    }
}

$ints = [PHP_INT_MIN, -1, 0, 1, PHP_INT_MAX, null];
$floats = [-INF, -1.5, -0.0, 0.0, 1.5, INF, NAN, -NAN, null];
$utc = new DateTimeZone('UTC');
$datetimes = [
    new DateTimeImmutable('2024-01-01 00:00:00', $utc),
    new DateTimeImmutable('2024-01-01 01:00:00', new DateTimeZone('+01:00')),
    new DateTimeImmutable('2023-12-31 23:59:59.999999', $utc),
    new DateTimeImmutable('2024-01-01 00:00:00', new DateTimeZone('America/New_York')),
    null,
];
$dates = [new DateTimeImmutable('2024-01-01', $utc), new DateTimeImmutable('2023-12-31', $utc), new DateTimeImmutable('1970-01-01', $utc), null];
$pairs = [
    'integer' => [type_integer(), $ints, type_integer(), $ints],
    'float' => [type_float(), $floats, type_float(), $floats],
    'integer and float' => [type_integer(), [9_007_199_254_740_993, 9_007_199_254_740_992, -3, 0, null], type_float(), [9_007_199_254_740_992.0, -3.0, 0.5, -0.0, NAN, INF, null]],
    'float and integer' => [type_float(), [9_007_199_254_740_992.0, -3.0, 0.5, -0.0, NAN, -INF, null], type_integer(), [9_007_199_254_740_993, -3, 0, null]],
    'boolean' => [type_boolean(), [true, false, null], type_boolean(), [true, false, null]],
    'string' => [type_string(), ['', 'a', 'B', 'b', 'ą', "a\0", 'aa', null], type_string(), ['', 'a', 'B', 'b', 'ą', "a\0", 'aa', null]],
    'numeric_string' => [type_numeric_string(), ['1', '1.0', '10', '9', '-0', null], type_numeric_string(), ['1', '1.0', '10', '9', '-0', null]],
    'json' => [type_json(), ['{"a":1}', '[]', '{"a":2}', null], type_json(), ['{"a":1}', '[]', '{"a":2}', null]],
    'datetime' => [type_datetime(), $datetimes, type_datetime(), $datetimes],
    'date' => [type_date(), $dates, type_date(), $dates],
    'time' => [type_time(), [new DateInterval('PT1H'), new DateInterval('PT59M60S'), new DateInterval('P1D'), null], type_time(), [new DateInterval('PT1H'), new DateInterval('PT0S'), null]],
    'uuid' => [type_uuid(), [new Uuid('00000000-0000-4000-8000-000000000000'), new Uuid('ffffffff-ffff-4fff-bfff-ffffffffffff'), null], type_uuid(), [new Uuid('00000000-0000-4000-8000-000000000000'), new Uuid('7fffffff-ffff-4fff-bfff-ffffffffffff'), null]],
    'date and datetime' => [type_date(), $dates, type_datetime(), $datetimes],
    'string and integer' => [type_string(), ['1', '1.5', 'x', null], type_integer(), [1, 2, null]],
    'integer and string' => [type_integer(), [1, 2, null], type_string(), ['1', ' 2', null]],
];
$php = new PhpBackend();
$rust = new RustBackend();

foreach ($pairs as $label => [$leftType, $leftValues, $rightType, $rightValues]) {
    $lefts = [];
    $rights = [];

    foreach ($leftValues as $left) {
        foreach ($rightValues as $right) {
            $lefts[] = $left;
            $rights[] = $right;
        }
    }

    $mismatches = [];
    $class = '';

    foreach (Operator::cases() as $operator) {
        $expected = compared(fn () => $php->compare(column($php, $leftType, $lefts), $operator, column($php, $rightType, $rights), 'l', 'r'));
        $actual = compared(function () use ($rust, $leftType, $lefts, $operator, $rightType, $rights, &$class) {
            $column = $rust->compare(column($rust, $leftType, $lefts), $operator, column($rust, $rightType, $rights), 'l', 'r');
            $class = get_class($column);

            return $column;
        });

        if ($expected !== $actual) {
            $mismatches[] = $operator->value . "\n  php:  " . $expected . "\n  rust: " . $actual;
        }
    }

    foreach (['isNull', 'isNotNull'] as $test) {
        if (compared(fn () => $php->{$test}(column($php, $leftType, $leftValues))) !== compared(fn () => $rust->{$test}(column($rust, $leftType, $leftValues)))) {
            $mismatches[] = $test;
        }
    }

    echo $label, ': ', $mismatches === [] ? 'same, ' . $class : "MISMATCH\n" . implode("\n", $mismatches), "\n";
}

$constant = $rust->constant(Definitions::of('c', type_integer(), true), 7, 3);
echo get_class($constant), ' ', json_encode($constant->values()), ' ', json_encode($rust->constant(Definitions::of('c', type_integer(), true), null, 2)->values()), ' ', $rust->constant(Definitions::of('c', type_integer()), 1, 0)->count(), "\n";
?>
--EXPECT--
integer: same, Flow\ETL\Values\Column\RustColumn
float: same, Flow\ETL\Values\Column\RustColumn
integer and float: same, Flow\ETL\Values\Column\RustColumn
float and integer: same, Flow\ETL\Values\Column\RustColumn
boolean: same, Flow\ETL\Values\Column\RustColumn
string: same, Flow\ETL\Values\Column\RustColumn
numeric_string: same, Flow\ETL\Values\Column\RustColumn
json: same, Flow\ETL\Values\Column\RustColumn
datetime: same, Flow\ETL\Values\Column\RustColumn
date: same, Flow\ETL\Values\Column\RustColumn
time: same, Flow\ETL\Values\Column\RustColumn
uuid: same, Flow\ETL\Values\Column\RustColumn
date and datetime: same, Flow\ETL\Values\Column\RustColumn
string and integer: same, Flow\ETL\Values\Column\RustColumn
integer and string: same, Flow\ETL\Values\Column\RustColumn
Flow\ETL\Values\Column\RustColumn [7,7,7] [null,null] 0
