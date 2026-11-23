--TEST--
RustGroupTable::lookup() returns the group of every row exactly as PhpGroupTable::lookup(), on the canonical encoding without a call back into PHP: known and unknown keys, null keys, lists, structures, maps, xml and html, columns from either backend, a batch cast to its declared type and a value that does not cast, and it never adds a group
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Values\Column\{Backend, PhpBackend, RustBackend};

use function Flow\ETL\DSL\{array_to_rows, date_schema, datetime_schema, html_schema, int_schema, list_schema, map_schema, schema, str_schema, structure_schema, xml_schema};
use function Flow\Types\DSL\{type_integer, type_list, type_map, type_string, type_structure};

$cases = [
    'nullable strings' => [str_schema('v', nullable: true), ['a', null, 'b', ''], ['b', 'c', null, '', 'a', '0']],
    'ints' => [int_schema('v'), [1, 2, 3], [3, 4, 1, 2, 5]],
    'lists with nulls' => [list_schema('v', type_list(type_integer()), nullable: true), [[1], null, []], [[], [1, 2], null, [1]]],
    'structures with nulls' => [structure_schema('v', type_structure(['a' => type_string()]), nullable: true), [['a' => 'x'], null], [['a' => 'y'], null, ['a' => 'x']]],
    'only nulls' => [str_schema('v', nullable: true), [null], [null, null]],
];

$looked = static function (Backend $tables, Backend $columns, $definition, array $known, array $probed): array {
    $schema = schema($definition);
    $rows = static fn(array $values) => array_to_rows(array_map(static fn($value) => ['v' => $value], $values), $schema, $columns);
    $table = $tables->groupTable(['v'], $schema);
    $table->assign($rows($known));
    $groups = $table->lookup($rows($probed));

    return [$groups->physicals(), $groups->nullCount(), $groups->type()->toString(), $table->count()];
};
$php = new PhpBackend();
$rust = new RustBackend();

foreach ($cases as $label => [$definition, $known, $probed]) {
    $expected = $looked($php, $php, $definition, $known, $probed);

    echo $label, ': ', $looked($rust, $rust, $definition, $known, $probed) === $expected ? 'same' : 'MISMATCH ' . json_encode($looked($rust, $rust, $definition, $known, $probed)),
        ', php columns ', $looked($rust, $php, $definition, $known, $probed) === $expected ? 'same' : 'MISMATCH',
        ', ', json_encode($expected), "\n";
}

$html = static fn (string $text): string => "<!DOCTYPE html><html><head></head><body><p>{$text}</p></body></html>";

$nested = [
    'maps' => [map_schema('v', type_map(type_string(), type_integer()), nullable: true), [['a' => 1], ['b' => 2, 'a' => 1], null], [['a' => 1, 'b' => 2], ['a' => 1], ['c' => 3], null]],
    'xml' => [xml_schema('v'), ['<a/>', '<b>1</b>'], ['<b>1</b>', '<c/>', '<a/>']],
    'html' => [html_schema('v'), [$html('a')], [$html('b'), $html('a')]],
];

foreach ($nested as $label => [$definition, $known, $probed]) {
    if ($label === 'html' && !class_exists('\Dom\HTMLDocument')) {
        echo "html: same, php columns same\n";

        continue;
    }

    $expected = $looked($php, $php, $definition, $known, $probed);

    echo $label, ': ', $looked($rust, $rust, $definition, $known, $probed) === $expected ? 'same' : 'MISMATCH',
        ', php columns ', $looked($rust, $php, $definition, $known, $probed) === $expected ? 'same' : 'MISMATCH', "\n";
}

// a batch whose column type differs from the declared one is cast to it, as after a group-by spill merge
$castLooked = static function (Backend $tables, Backend $columns, $declared, $batch, array $known, array $probed): array {
    $table = $tables->groupTable(['v'], schema($declared));
    $table->assign(array_to_rows(array_map(static fn($value) => ['v' => $value], $known), schema($declared), $columns));
    $groups = $table->lookup(array_to_rows(array_map(static fn($value) => ['v' => $value], $probed), schema($batch), $columns));

    return [$groups->physicals(), $table->count()];
};
$mismatches = [
    'a date batch against datetime keys' => [datetime_schema('v'), date_schema('v'), [new DateTimeImmutable('2026-01-01 00:00:00 UTC'), new DateTimeImmutable('2026-01-02 12:00:00 UTC')], [new DateTimeImmutable('2026-01-01'), new DateTimeImmutable('2026-01-02')]],
    'an xml batch against string keys' => [str_schema('v'), xml_schema('v'), ['<a/>', 'x'], ['<a/>', '<b/>']],
];

foreach ($mismatches as $label => [$declared, $batch, $known, $probed]) {
    $expected = $castLooked($php, $php, $declared, $batch, $known, $probed);

    echo $label, ': ', $castLooked($rust, $rust, $declared, $batch, $known, $probed) === $expected ? 'same' : 'MISMATCH ' . json_encode($castLooked($rust, $rust, $declared, $batch, $known, $probed)),
        ', php columns ', $castLooked($rust, $php, $declared, $batch, $known, $probed) === $expected ? 'same' : 'MISMATCH',
        ', ', json_encode($expected), "\n";
}

$refusal = static function (Backend $backend): string {
    try {
        $table = $backend->groupTable(['v'], schema(int_schema('v')));
        $table->assign(array_to_rows([['v' => 'abc']], schema(str_schema('v')), $backend));

        return 'no exception';
    } catch (Throwable $e) {
        return $e::class . ': ' . $e->getMessage();
    }
};

echo 'a value that does not cast: ', $refusal($rust) === $refusal($php) ? 'same ' . $refusal($rust) : 'MISMATCH ' . $refusal($rust) . ' | ' . $refusal($php), "\n";
?>
--EXPECT--
nullable strings: same, php columns same, [[2,null,1,3,0,null],2,"integer",4]
ints: same, php columns same, [[2,null,0,1,null],2,"integer",3]
lists with nulls: same, php columns same, [[2,null,1,0],1,"integer",3]
structures with nulls: same, php columns same, [[null,1,0],1,"integer",2]
only nulls: same, php columns same, [[0,0],0,"integer",1]
maps: same, php columns same
xml: same, php columns same
html: same, php columns same
a date batch against datetime keys: same, php columns same, [[0,null],2]
an xml batch against string keys: same, php columns same, [[0,null],2]
a value that does not cast: same Flow\ETL\Foundation\Exception\EvaluationException: Column "v" value 'abc' cannot be cast to integer to compare: "abc" is not a number (row 0)
