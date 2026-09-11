--TEST--
both Floe encoders refuse a null on a NOT-NULL column with the same class and message
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Rows;
use Flow\Floe\NativeFloeEncoder;
use Flow\Floe\PhpFloeEncoder;

$schema = schema(int_schema('id'), str_schema('name'), str_schema('note', nullable: true));

// Rows refuses a null on a NOT-NULL column, so the TypedRowValues are built directly - this pins
// what each encoder does when handed one anyway.
$refused = [
    new TypedRowValues(['id' => 1, 'name' => 'a', 'note' => null], ['id' => type_integer(), 'name' => type_string(), 'note' => type_string()]),
    new TypedRowValues(['id' => 2, 'name' => null, 'note' => null], ['id' => type_integer(), 'name' => type_string(), 'note' => type_string()]),
];

$php = new PhpFloeEncoder($schema);
$native = new NativeFloeEncoder($schema);

$describe = static function (callable $encode): string {
    try {
        $encode();

        return 'NO EXCEPTION';
    } catch (Throwable $e) {
        return $e::class . '|' . $e->getMessage();
    }
};

$phpRefusal = $describe(static fn() => $php->encode($refused));
$nativeRefusal = $describe(static fn() => $native->encode($refused));

printf("refusal:%s\n", $phpRefusal === $nativeRefusal ? 'match' : "MISMATCH php[{$phpRefusal}] native[{$nativeRefusal}]");
printf("%s\n", $phpRefusal);

// a null under a nullable declaration still encodes, on both sides, to the same bytes
$allowed = (new PhpRowHydrator())->dehydrate(
    new Rows($schema, row(['id' => 1, 'name' => 'a', 'note' => null])),
);

printf("nullable:%s\n", $php->encode($allowed) === $native->encode($allowed) ? 'identical' : 'DIFFERENT');
?>
--EXPECT--
refusal:match
Flow\ETL\Exception\SchemaMismatchException|Rows do not match their schema: column "name" (row 1): could not convert null to string, column is not nullable
nullable:identical
