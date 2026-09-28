--TEST--
DefaultBackend::decode() refuses every corrupt buffer set PhpBackend::decode() refuses, with its class and message
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Column\DefaultBackend;
use Flow\ETL\Column\PhpBackend;

use function Flow\ETL\DSL\{int_schema, list_schema, map_schema, null_schema, str_schema, structure_schema};
use function Flow\Types\DSL\{type_integer, type_list, type_map, type_null, type_optional, type_string, type_structure};

$cases = [
    'too few buffers' => [int_schema('a'), [''], 1, 0],
    'short fixed buffer' => [int_schema('a'), ['', "\x01\x00"], 1, 0],
    'offsets not from 0' => [str_schema('a'), ['', "\x01\x00\x00\x00\x02\x00\x00\x00", 'ab'], 1, 0],
    'offsets not monotonic' => [str_schema('a'), ['', "\x00\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00", 'ab'], 2, 0],
    'offsets past the data' => [str_schema('a'), ['', "\x00\x00\x00\x00\x03\x00\x00\x00", 'ab'], 1, 0],
    'short offsets' => [str_schema('a'), ['', "\x00\x00", ''], 1, 0],
    'map entries validity present' => [map_schema('a', type_map(type_string(), type_integer())), ['', "\x00\x00\x00\x00\x00\x00\x00\x00", "\x01", '', "\x00\x00\x00\x00", '', '', ''], 1, 0],
    'null count disagreeing with the bitmap' => [int_schema('a', nullable: true), ["\x01", str_repeat("\x00", 16)], 2, 0],
    'values shorter than the rows' => [int_schema('a', nullable: true), ['', ''], 9, 0],
    'short validity bitmap' => [int_schema('a', nullable: true), ["\x01", str_repeat("\x00", 72)], 9, 0],
    'null column with a wrong null count' => [null_schema('a'), [], 2, 1],
    'leftover buffers' => [int_schema('a'), ['', "\x01\x00\x00\x00\x00\x00\x00\x00", ''], 1, 0],
    'list element nulls' => [list_schema('a', type_list(type_integer())), ['', "\x00\x00\x00\x00\x01\x00\x00\x00", "\x00", str_repeat("\x00", 8)], 1, 0],
    'list offsets too short' => [list_schema('a', type_list(type_integer())), ['', "\x00\x00\x00\x00", '', ''], 1, 0],
    'map key nulls' => [map_schema('a', type_map(type_integer(), type_integer())), ['', "\x00\x00\x00\x00\x01\x00\x00\x00", '', "\x00", str_repeat("\x00", 8), '', str_repeat("\x00", 8)], 1, 0],
    'map value nulls' => [map_schema('a', type_map(type_integer(), type_list(type_integer()))), ['', "\x00\x00\x00\x00\x01\x00\x00\x00", '', '', str_repeat("\x00", 8), "\x00", "\x00\x00\x00\x00\x00\x00\x00\x00", '', ''], 1, 0],
    'nested list of a structure' => [structure_schema('a', type_structure(['l' => type_list(type_string())])), ['', '', "\x00\x00\x00\x00\x01\x00\x00\x00", "\x00", "\x00\x00\x00\x00\x00\x00\x00\x00", ''], 1, 0],
    'optional list element nulls pass' => [list_schema('a', type_list(type_optional(type_integer()))), ['', "\x00\x00\x00\x00\x01\x00\x00\x00", "\x00", str_repeat("\x00", 8)], 1, 0],
    'null list element passes' => [list_schema('a', type_list(type_null())), ['', "\x00\x00\x00\x00\x02\x00\x00\x00"], 1, 0],
];

foreach ($cases as $label => [$definition, $buffers, $count, $nullCount]) {
    $php = outcome(static fn() => (new PhpBackend())->decode($definition, $buffers, $count, $nullCount)->values());
    $native = outcome(static fn() => (new DefaultBackend())->decode($definition, $buffers, $count, $nullCount)->values());

    echo $label, ': ', $php === $native ? $native : "DIFF\n  php:    {$php}\n  native: {$native}", "\n";
}
?>
--EXPECT--
too few buffers: Flow\ETL\Exception\InvalidArgumentException: Column buffers exhausted: the column needs more buffers than given
short fixed buffer: Flow\ETL\Exception\InvalidArgumentException: Int64 values buffer of 2 bytes, expected 8 for 1 rows
offsets not from 0: Flow\ETL\Exception\InvalidArgumentException: Utf8 offsets start at 1, not 0
offsets not monotonic: Flow\ETL\Exception\InvalidArgumentException: Utf8 offsets are not monotonic: offset 1 is 2, offset 2 is 1
offsets past the data: Flow\ETL\Exception\InvalidArgumentException: Utf8 data buffer of 2 bytes, the last offset is 3
short offsets: Flow\ETL\Exception\InvalidArgumentException: Utf8 offsets buffer of 2 bytes, expected 8 for 1 rows
map entries validity present: Flow\ETL\Exception\InvalidArgumentException: Map entries validity must be omitted, got 1 bytes
null count disagreeing with the bitmap: Flow\ETL\Exception\InvalidArgumentException: Column null count 0 disagrees with its validity bitmap (1 nulls in 2 rows)
values shorter than the rows: Flow\ETL\Exception\InvalidArgumentException: Int64 values buffer of 0 bytes, expected 72 for 9 rows
short validity bitmap: Flow\ETL\Exception\InvalidArgumentException: Validity bitmap of 1 bytes is too short for 9 rows
null column with a wrong null count: Flow\ETL\Exception\InvalidArgumentException: Null column of 2 rows carries a null count of 1
leftover buffers: Flow\ETL\Exception\InvalidArgumentException: Column "a": 1 buffers left after decoding
list element nulls: Flow\ETL\Exception\InvalidArgumentException: List child holds 1 nulls in a non-nullable integer
list offsets too short: Flow\ETL\Exception\InvalidArgumentException: List offsets buffer of 4 bytes, expected 8 for 1 rows
map key nulls: Flow\ETL\Exception\InvalidArgumentException: Map key child holds 1 nulls in a non-nullable integer
map value nulls: Flow\ETL\Exception\InvalidArgumentException: Map value child holds 1 nulls in a non-nullable list<integer>
nested list of a structure: Flow\ETL\Exception\InvalidArgumentException: List child holds 1 nulls in a non-nullable string
optional list element nulls pass: a:1:{i:0;a:1:{i:0;N;}}
null list element passes: a:1:{i:0;a:2:{i:0;N;i:1;N;}}
