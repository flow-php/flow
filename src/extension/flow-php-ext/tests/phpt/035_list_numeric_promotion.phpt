--TEST--
list<float> promotes long elements and list<?float> keeps a null, identically in both hydrators
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;

use Flow\ETL\Row\NativeRowHydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;

// TypeDetector unifies an int|float element set to float, so list<float> is a schema a
// schemaless source can produce. The cast door has to promote every long element to a double,
// and it has to do it the same way on both sides of the extension boundary.
$php = new PhpRowHydrator();
$native = new NativeRowHydrator();

$cases = [
    'longs' => [
        schema(list_schema('l', type_list(type_float()))),
        [new RawRowValues(['l' => [33, 65.5]]), new RawRowValues(['l' => [1, 2, 3]])],
    ],
    'nullable' => [
        schema(list_schema('l', type_list(type_optional(type_float())))),
        [new RawRowValues(['l' => [1, null, 2.5]]), new RawRowValues(['l' => [null]])],
    ],
    'numeric_strings' => [
        schema(list_schema('l', type_list(type_float()))),
        [new RawRowValues(['l' => ['1', 2, '3.5']])],
    ],
    'empty' => [
        schema(list_schema('l', type_list(type_float()))),
        [new RawRowValues(['l' => []])],
    ],
];

foreach ($cases as $label => [$s, $batch]) {
    $phpRows = $php->hydrate($batch, $s);
    $nativeRows = $native->hydrate($batch, $s);

    printf("%-16s parity:%s\n", $label, serialize($phpRows) === serialize($nativeRows) ? 'yes' : 'NO');

    foreach ($phpRows->all() as $i => $row) {
        printf(
            "%-16s row%d php:%s native:%s\n",
            $label,
            $i,
            json_encode($row->get('l'), JSON_PRESERVE_ZERO_FRACTION),
            json_encode($nativeRows->all()[$i]->get('l'), JSON_PRESERVE_ZERO_FRACTION),
        );
    }
}
?>
--EXPECT--
longs            parity:yes
longs            row0 php:[33.0,65.5] native:[33.0,65.5]
longs            row1 php:[1.0,2.0,3.0] native:[1.0,2.0,3.0]
nullable         parity:yes
nullable         row0 php:[1.0,null,2.5] native:[1.0,null,2.5]
nullable         row1 php:[null] native:[null]
numeric_strings  parity:yes
numeric_strings  row0 php:[1.0,2.0,3.5] native:[1.0,2.0,3.5]
empty            parity:yes
empty            row0 php:[] native:[]
