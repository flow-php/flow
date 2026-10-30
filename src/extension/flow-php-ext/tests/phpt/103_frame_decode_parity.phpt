--TEST--
RustBackend::decodeFrame() decodes every frame PhpBackend::decodeFrame() decodes and refuses every frame it refuses, with the same class, message and previous
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Column\RustBackend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Column\Frame\Codec;
use Flow\ETL\Values\Column\Frame\NoopCodec;

use function Flow\ETL\DSL\{array_to_rows, int_schema, list_schema, map_schema, null_schema, schema, str_schema, structure_schema};
use function Flow\Types\DSL\{type_integer, type_list, type_map, type_null, type_optional, type_string, type_structure};

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

/**
 * @param array<int, int> $words directory word index (0 = row count) => new value
 */
function patch(string $body, array $words): string
{
    foreach ($words as $index => $word) {
        $body = substr_replace($body, pack('V', $word), 4 * $index, 4);
    }

    return $body;
}

/**
 * @return list<int> the row, node and buffer counts, then every node and extent word
 */
function directory(string $body): array
{
    $counts = unpack('V3', $body);

    return array_values((array) unpack('V' . (3 + (2 * ($counts[2] + $counts[3]))), $body));
}

function decoded(string $body, Schema $schema, Codec $codec, bool $native): array
{
    $backend = $native ? new RustBackend() : new PhpBackend();

    return $backend->decodeFrame($body, $schema, $codec)->toArray();
}

$worked = array_to_rows([['id' => 1, 'name' => 'ab'], ['id' => 2, 'name' => null]], schema(int_schema('id'), str_schema('name', nullable: true)));
$cases = [
    'all column types' => php_rows(all_types_schema(), all_types_values()),
    'worked example' => $worked,
    'list with a null slot' => array_to_rows([['l' => [1, 2]], ['l' => null], ['l' => []]], schema(list_schema('l', type_list(type_integer()), nullable: true))),
    'map' => array_to_rows([['m' => ['a' => 1]], ['m' => ['b' => 2, 'c' => 3]]], schema(map_schema('m', type_map(type_string(), type_integer())))),
    'structure with a null parent' => array_to_rows([['s' => ['a' => 1, 'b' => 'x']], ['s' => null]], schema(structure_schema('s', type_structure(['a' => type_integer(), 'b' => type_string()]), nullable: true))),
    'null column' => array_to_rows([['n' => null], ['n' => null]], schema(null_schema('n'))),
    'list of nulls' => array_to_rows([['l' => [null, null]], ['l' => [null]]], schema(list_schema('l', type_list(type_null())))),
    'map of null values' => array_to_rows([['m' => ['a' => null]]], schema(map_schema('m', type_map(type_string(), type_null())))),
    'numeric column name' => array_to_rows([['0' => 1], ['0' => null]], schema(int_schema('0', nullable: true))),
    'no columns' => array_to_rows([[], [], []], schema()),
];

foreach ($cases as $name => $rows) {
    foreach (['noop' => new NoopCodec(), 'trailing zeros' => new TrailingZerosCodec()] as $codecName => $codec) {
        $body = (new PhpBackend())->encodeFrame($rows, $codec);
        echo "{$name} / {$codecName}: ";
        assert_same_outcome(
            static fn(): array => decoded($body, $rows->schema(), $codec, false),
            static fn(): array => decoded($body, $rows->schema(), $codec, true),
        );
    }
}

$body = (new PhpBackend())->encodeFrame($worked, new NoopCodec());
$list = array_to_rows([['l' => [1, 2]], ['l' => null]], schema(list_schema('l', type_list(type_integer()), nullable: true)));
$mapRows = array_to_rows([['m' => [1 => null]]], schema(map_schema('m', type_map(type_integer(), type_optional(type_integer())))));
$mapWords = directory((new PhpBackend())->encodeFrame($mapRows, new NoopCodec()));
$refusals = [
    'a body shorter than the directory counts' => [substr($body, 0, 11), $worked->schema(), new NoopCodec()],
    'a directory cut short' => [substr($body, 0, 40), $worked->schema(), new NoopCodec()],
    'a node count other than the schema' => [patch($body, [1 => 3]), $worked->schema(), new NoopCodec()],
    'a buffer count other than the schema' => [patch($body, [2 => 4]), $worked->schema(), new NoopCodec()],
    'a column of another row count' => [patch($body, [3 => 3]), $worked->schema(), new NoopCodec()],
    'more nulls than rows' => [patch($body, [4 => 3]), $worked->schema(), new NoopCodec()],
    'a buffer past the area' => [patch($body, [10 => 200]), $worked->schema(), new NoopCodec()],
    'a buffer shorter than its prefix' => [patch($body, [10 => 4]), $worked->schema(), new NoopCodec()],
    'a null count other than the bitmap' => [patch($body, [6 => 0]), $worked->schema(), new NoopCodec()],
    'a cut values buffer' => [patch($body, [10 => 10]), $worked->schema(), new NoopCodec()],
    'a buffer that decodes to another length than its prefix' => [
        substr_replace((new PhpBackend())->encodeFrame($worked, new TrailingZerosCodec()), pack('P', 15), 72, 8),
        $worked->schema(),
        new TrailingZerosCodec(),
    ],
    'child nodes that disagree with the buffers' => [patch((new PhpBackend())->encodeFrame($list, new NoopCodec()), [5 => 3]), $list->schema(), new NoopCodec()],
    'a not null column with a null node' => [
        (new PhpBackend())->encodeFrame(array_to_rows([['id' => 1], ['id' => null]], schema(int_schema('id', nullable: true))), new NoopCodec()),
        schema(int_schema('id')),
        new NoopCodec(),
    ],
    'a null in a non-optional list element' => [
        (new PhpBackend())->encodeFrame(array_to_rows([['l' => [1, null]]], schema(list_schema('l', type_list(type_optional(type_integer()))))), new NoopCodec()),
        schema(list_schema('l', type_list(type_integer()))),
        new NoopCodec(),
    ],
    'a null map key' => [
        patch((new PhpBackend())->encodeFrame($mapRows, new NoopCodec()), [8 => 1, 10 => 0, 17 => $mapWords[21], 18 => $mapWords[22], 21 => $mapWords[17], 22 => $mapWords[18]]),
        $mapRows->schema(),
        new NoopCodec(),
    ],
];

foreach ($refusals as $name => [$corrupt, $schema, $codec]) {
    echo "{$name}: ";
    $php = outcome(static fn(): array => decoded($corrupt, $schema, $codec, false));
    $native = outcome(static fn(): array => decoded($corrupt, $schema, $codec, true));
    echo $php === $native && refused($php) ? "identical\n" : "php:    {$php}\nnative: {$native}\n";
}
?>
--EXPECT--
all column types / noop: identical
all column types / trailing zeros: identical
worked example / noop: identical
worked example / trailing zeros: identical
list with a null slot / noop: identical
list with a null slot / trailing zeros: identical
map / noop: identical
map / trailing zeros: identical
structure with a null parent / noop: identical
structure with a null parent / trailing zeros: identical
null column / noop: identical
null column / trailing zeros: identical
list of nulls / noop: identical
list of nulls / trailing zeros: identical
map of null values / noop: identical
map of null values / trailing zeros: identical
numeric column name / noop: identical
numeric column name / trailing zeros: identical
no columns / noop: identical
no columns / trailing zeros: identical
a body shorter than the directory counts: identical
a directory cut short: identical
a node count other than the schema: identical
a buffer count other than the schema: identical
a column of another row count: identical
more nulls than rows: identical
a buffer past the area: identical
a buffer shorter than its prefix: identical
a null count other than the bitmap: identical
a cut values buffer: identical
a buffer that decodes to another length than its prefix: identical
child nodes that disagree with the buffers: identical
a not null column with a null node: identical
a null in a non-optional list element: identical
a null map key: identical
