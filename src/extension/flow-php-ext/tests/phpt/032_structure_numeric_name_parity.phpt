--TEST--
a numeric element name ("0" on the wire, always a JSON string) keeps its DeclaredKey::Index encoding and its cast fallback
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

use Flow\ETL\Row\NativeRowHydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;

$numericNamed = type_structure([0 => type_integer(), 'b' => type_string()]);

$rows = rows(schema(structure_schema('st', $numericNamed)), row(['st' => [0 => 1, 'b' => 'x']]));

printf("frames-identical:%s\n", php_frames($rows) === ext_frames($rows) ? 'yes' : 'NO');

$frames = php_frames($rows);
assert_rows_identical(php_decode_frames($frames), ext_decode_frames($frames));

// build_structure_kind still bails to CastKind::Fallback for a numeric name -
// parity with the pure-PHP hydrator proves it took the same code path.
$schema = schema(structure_schema('st', $numericNamed));
$batch = [new RawRowValues(['st' => [0 => '1', 'b' => 2]])];

printf(
    "cast-parity:%s\n",
    serialize((new PhpRowHydrator())->cast($batch, $schema)) === serialize((new NativeRowHydrator())->cast($batch, $schema)) ? 'yes' : 'NO',
);
?>
--EXPECT--
frames-identical:yes
identical
cast-parity:yes
