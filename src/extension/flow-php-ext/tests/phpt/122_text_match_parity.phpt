--TEST--
RustBackend::startsWith(), endsWith() and contains() answer as PhpBackend does: values, null count and type, over nulls on either side, multibyte text, raw bytes, a constant needle, sliced columns, columns of either backend and an empty column
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Values\Column\{Backend, Column, PhpBackend, RustBackend};

use function Flow\ETL\DSL\{array_to_rows, schema, str_schema};

$php = new PhpBackend();
$rust = new RustBackend();
$generated = array_map(
    static fn(int $i): array => [$i % 7 === 0 ? null : str_repeat(chr(97 + $i % 3), $i % 5), $i % 11 === 0 ? null : str_repeat(chr(97 + $i % 2), $i % 3)],
    range(0, 200),
);
$cases = [
    'probe rows' => [['abcx', 'x'], [null, 'x'], ['abc', null], ['abc', ''], ['', ''], ['żółw', 'w'], ['x', 'xx']],
    'generated strings with nulls on both sides' => $generated,
    'multibyte text' => [['zażółć gęślą', 'zażó'], ['zażółć gęślą', 'ślą'], ['zażółć gęślą', 'ółć g'], ['żółw', 'ół'], ['日本語', '本']],
    'raw bytes' => [["a\0b", "\0"], ["a\0b", "a\0"], ["\xff\xfe", "\xfe"], ["\xff\xfe", "\xff"], ["\0", "\0\0"], ["x\xffy", "\xffy"]],
];
$columns = static function (Backend $backend, array $pairs): array {
    $rows = array_to_rows(
        array_map(static fn(array $pair): array => ['h' => $pair[0], 'n' => $pair[1]], $pairs),
        schema(str_schema('h', nullable: true), str_schema('n', nullable: true)),
        $backend,
    );

    return [$rows->column('h'), $rows->column('n')];
};
$described = static fn(Column $column): string => json_encode([$column->values(), $column->nullCount(), $column->type()->toString()]);
$same = static function (string $method, Column $rustHaystack, Column $rustNeedle, Column $phpHaystack, Column $phpNeedle) use ($php, $rust, $described): string {
    $expected = $described($php->{$method}($phpHaystack, $phpNeedle));
    $actual = $described($rust->{$method}($rustHaystack, $rustNeedle));

    return $actual === $expected ? 'same' : "MISMATCH {$actual} {$expected}";
};

foreach (['startsWith', 'endsWith', 'contains'] as $method) {
    foreach ($cases as $label => $pairs) {
        [$rustHaystack, $rustNeedle] = $columns($rust, $pairs);
        [$phpHaystack, $phpNeedle] = $columns($php, $pairs);
        $count = count($pairs);

        echo $method, ' ', $label, ': ', implode(', ', array_unique([
            $same($method, $rustHaystack, $rustNeedle, $phpHaystack, $phpNeedle),
            $same($method, $rustHaystack->slice(1, $count - 1), $rustNeedle->slice(1, $count - 1), $phpHaystack->slice(1, $count - 1), $phpNeedle->slice(1, $count - 1)),
            $same($method, $rustHaystack, $phpNeedle, $phpHaystack, $phpNeedle),
            $same($method, $phpHaystack, $rustNeedle, $phpHaystack, $phpNeedle),
        ])), "\n";
    }

    [$rustHaystack] = $columns($rust, $cases['probe rows']);
    [$phpHaystack] = $columns($php, $cases['probe rows']);
    echo $method, ' a constant needle: ', $same(
        $method,
        $rustHaystack,
        $rust->constant(str_schema('n'), 'x', 7),
        $phpHaystack,
        $php->constant(str_schema('n'), 'x', 7),
    ), "\n";

    [$rustHaystack, $rustNeedle] = $columns($rust, []);
    [$phpHaystack, $phpNeedle] = $columns($php, []);
    echo $method, ' an empty column: ', $same($method, $rustHaystack, $rustNeedle, $phpHaystack, $phpNeedle), "\n";
}
?>
--EXPECT--
startsWith probe rows: same
startsWith generated strings with nulls on both sides: same
startsWith multibyte text: same
startsWith raw bytes: same
startsWith a constant needle: same
startsWith an empty column: same
endsWith probe rows: same
endsWith generated strings with nulls on both sides: same
endsWith multibyte text: same
endsWith raw bytes: same
endsWith a constant needle: same
endsWith an empty column: same
contains probe rows: same
contains generated strings with nulls on both sides: same
contains multibyte text: same
contains raw bytes: same
contains a constant needle: same
contains an empty column: same
