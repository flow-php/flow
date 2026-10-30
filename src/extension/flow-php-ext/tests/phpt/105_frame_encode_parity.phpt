--TEST--
RustBackend::encodeFrame() writes the bytes PhpBackend::encodeFrame() writes, for rows built by either backend
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Column\RustBackend;
use Flow\ETL\Values\Column\Frame\Codec;
use Flow\ETL\Values\Column\Frame\NoopCodec;

use function Flow\ETL\DSL\{array_to_rows, int_schema, list_schema, map_schema, null_schema, schema, str_schema, structure_schema};
use function Flow\Types\DSL\{type_integer, type_list, type_map, type_null, type_string, type_structure};

/**
 * Shrinks a buffer ending in zero bytes, so the per-buffer length prefix and the codec callback are exercised.
 */
final class TrailingZerosCodec implements Codec
{
    public function decode(string $data): string
    {
        return substr($data, 0, -1) . str_repeat("\0", ord($data[strlen($data) - 1]));
    }

    public function encode(string $data): string
    {
        $trimmed = rtrim($data, "\0");

        return $trimmed . chr(strlen($data) - strlen($trimmed));
    }

    public function id(): int
    {
        return 0x00;
    }
}

$cases = [
    'all column types' => [all_types_schema(), all_types_values()],
    'worked example' => [schema(int_schema('id'), str_schema('name', nullable: true)), [['id' => 1, 'name' => 'ab'], ['id' => 2, 'name' => null]]],
    'list with a null slot' => [schema(list_schema('l', type_list(type_integer()), nullable: true)), [['l' => [1, 2]], ['l' => null], ['l' => []]]],
    'map' => [schema(map_schema('m', type_map(type_string(), type_integer()))), [['m' => ['a' => 1]], ['m' => ['b' => 2, 'c' => 3]]]],
    'structure with a null parent' => [schema(structure_schema('s', type_structure(['a' => type_integer(), 'b' => type_string()]), nullable: true)), [['s' => ['a' => 1, 'b' => 'x']], ['s' => null]]],
    'null column' => [schema(null_schema('n')), [['n' => null], ['n' => null]]],
    'list of nulls' => [schema(list_schema('l', type_list(type_null()))), [['l' => [null, null]], ['l' => [null]]]],
    'map of null values' => [schema(map_schema('m', type_map(type_string(), type_null()))), [['m' => ['a' => null]]]],
    'numeric column name' => [schema(int_schema('0', nullable: true)), [['0' => 1], ['0' => null]]],
    'empty batch' => [schema(int_schema('id'), str_schema('name')), []],
];

foreach ($cases as $name => [$schema, $values]) {
    foreach (['php rows' => php_rows($schema, $values), 'native rows' => native_rows($schema, $values)] as $built => $rows) {
        foreach (['noop' => new NoopCodec(), 'trailing zeros' => new TrailingZerosCodec()] as $codecName => $codec) {
            echo "{$name} / {$built} / {$codecName}: ";
            assert_same_outcome(
                static fn(): string => bin2hex((new PhpBackend())->encodeFrame($rows, $codec)),
                static fn(): string => bin2hex((new RustBackend())->encodeFrame($rows, $codec)),
            );
        }
    }
}

$columnless = array_to_rows([[], [], []], schema());
echo 'no columns: ';
assert_same_outcome(
    static fn(): string => bin2hex((new PhpBackend())->encodeFrame($columnless, new NoopCodec())),
    static fn(): string => bin2hex((new RustBackend())->encodeFrame($columnless, new NoopCodec())),
);
?>
--EXPECT--
all column types / php rows / noop: identical
all column types / php rows / trailing zeros: identical
all column types / native rows / noop: identical
all column types / native rows / trailing zeros: identical
worked example / php rows / noop: identical
worked example / php rows / trailing zeros: identical
worked example / native rows / noop: identical
worked example / native rows / trailing zeros: identical
list with a null slot / php rows / noop: identical
list with a null slot / php rows / trailing zeros: identical
list with a null slot / native rows / noop: identical
list with a null slot / native rows / trailing zeros: identical
map / php rows / noop: identical
map / php rows / trailing zeros: identical
map / native rows / noop: identical
map / native rows / trailing zeros: identical
structure with a null parent / php rows / noop: identical
structure with a null parent / php rows / trailing zeros: identical
structure with a null parent / native rows / noop: identical
structure with a null parent / native rows / trailing zeros: identical
null column / php rows / noop: identical
null column / php rows / trailing zeros: identical
null column / native rows / noop: identical
null column / native rows / trailing zeros: identical
list of nulls / php rows / noop: identical
list of nulls / php rows / trailing zeros: identical
list of nulls / native rows / noop: identical
list of nulls / native rows / trailing zeros: identical
map of null values / php rows / noop: identical
map of null values / php rows / trailing zeros: identical
map of null values / native rows / noop: identical
map of null values / native rows / trailing zeros: identical
numeric column name / php rows / noop: identical
numeric column name / php rows / trailing zeros: identical
numeric column name / native rows / noop: identical
numeric column name / native rows / trailing zeros: identical
empty batch / php rows / noop: identical
empty batch / php rows / trailing zeros: identical
empty batch / native rows / noop: identical
empty batch / native rows / trailing zeros: identical
no columns: identical
