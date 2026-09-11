--TEST--
enum, json and date columns round-trip identically
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

use Flow\Types\Value\Json;

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
    schema(
        int_schema('id'),
        enum_schema('backed', PhptSuit::class),
        enum_schema('pure', PhptDirection::class),
        json_schema('json_array'),
        json_schema('json_object'),
        date_schema('date'),
    ),
    row([
        'id' => 1,
        'backed' => PhptSuit::Hearts,
        'pure' => PhptDirection::Down,
        'json_array' => Json::fromArray([1, 2, ['a' => true]]),
        'json_object' => Json::fromArray(['name' => 'flow', 'nested' => ['x' => 1]], asObject: true),
        'date' => new DateTimeImmutable('2025-03-01'),
    ]),
);

$frames = php_frames($rows);
$actual = ext_decode_frames($frames);

assert_rows_identical(php_decode_frames($frames), $actual);

var_dump($actual[0]->get('backed') === PhptSuit::Hearts);
var_dump($actual[0]->get('pure') === PhptDirection::Down);
var_dump((string) $actual[0]->get('json_array'));
var_dump((string) $actual[0]->get('json_object'));
var_dump($actual[0]->get('date')->format('Y-m-d'));
?>
--EXPECT--
identical
bool(true)
bool(true)
string(16) "[1,2,{"a":true}]"
string(32) "{"name":"flow","nested":{"x":1}}"
string(10) "2025-03-01"
