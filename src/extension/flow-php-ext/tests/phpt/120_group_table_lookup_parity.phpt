--TEST--
RustGroupTable::lookup() returns the group of every row exactly as PhpGroupTable::lookup(): known and unknown keys, null keys, lists and structures, columns from either backend, and it never adds a group
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Values\Column\{Backend, PhpBackend, RustBackend};

use function Flow\ETL\DSL\{array_to_rows, int_schema, list_schema, schema, str_schema, structure_schema};
use function Flow\Types\DSL\{type_integer, type_list, type_string, type_structure};

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
?>
--EXPECT--
nullable strings: same, php columns same, [[2,null,1,3,0,null],2,"integer",4]
ints: same, php columns same, [[2,null,0,1,null],2,"integer",3]
lists with nulls: same, php columns same, [[2,null,1,0],1,"integer",3]
structures with nulls: same, php columns same, [[null,1,0],1,"integer",2]
only nulls: same, php columns same, [[0,0],0,"integer",1]
