--TEST--
enum, json and date columns round-trip identically
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';


use function Flow\ETL\DSL\{row, rows, int_entry, enum_entry, json_entry, json_object_entry, date_entry};

enum PhptSuit: string
{
    case Hearts = 'hearts';
    case Spades = 'spades';
}

enum PhptDirection
{
    case Up;
    case Down;
}

$rows = rows(
    row(
        int_entry('id', 1),
        enum_entry('backed', PhptSuit::Hearts),
        enum_entry('pure', PhptDirection::Down),
        json_entry('json_array', [1, 2, ['a' => true]]),
        json_object_entry('json_object', ['name' => 'flow', 'nested' => ['x' => 1]]),
        date_entry('date', new DateTimeImmutable('2025-03-01')),
    ),
);

$frames = php_frames($rows);
$actual = ext_decode_frames($frames);

assert_rows_identical(php_decode_frames($frames), $actual);

var_dump($actual[0]->get('backed')->value() === PhptSuit::Hearts);
var_dump($actual[0]->get('pure')->value() === PhptDirection::Down);
var_dump((string) $actual[0]->get('json_array')->value());
var_dump((string) $actual[0]->get('json_object')->value());
var_dump($actual[0]->get('date')->value()->format('Y-m-d'));
?>
--EXPECT--
identical
bool(true)
bool(true)
string(16) "[1,2,{"a":true}]"
string(32) "{"name":"flow","nested":{"x":1}}"
string(10) "2025-03-01"
