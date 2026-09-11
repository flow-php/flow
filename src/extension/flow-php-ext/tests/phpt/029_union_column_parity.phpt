--TEST--
NativeRowHydrator resolves a union column member per value, identically to PhpRowHydrator
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function Flow\Types\DSL\type_uuid;

use Flow\ETL\Row\NativeRowHydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema\Metadata;

use Flow\ETL\Schema\Definition\UnionDefinition;

$union = type_union(type_string(), type_integer());
$unmatchable = type_union(type_uuid(), type_datetime());

// values that already conform to a union member: hydrate() has nothing to convert.
$conforming = [
    'members' => [
        schema(int_schema('id'), new UnionDefinition('a', $union, true)),
        [
            new RawRowValues(['id' => 1, 'a' => 42]),
            new RawRowValues(['id' => 2, 'a' => 'x']),
            new RawRowValues(['id' => 3, 'a' => null]),
            new RawRowValues(['id' => 4]),
        ],
    ],
    'metadata' => [
        schema(new UnionDefinition('a', $union, true)),
        [
            new RawRowValues(['a' => 42], ['a' => Metadata::fromArray(['k' => 'v'])]),
            new RawRowValues(['a' => 'x'], ['a' => Metadata::fromArray(['k' => 'v'])]),
        ],
    ],
];

// values a union member accepts only after a cast
$castOnly = [
    'castable' => [
        schema(new UnionDefinition('a', $union, true)),
        [
            new RawRowValues(['a' => '42']),
            new RawRowValues(['a' => 1.5]),
            new RawRowValues(['a' => true]),
        ],
    ],
];

$php = new PhpRowHydrator();
$native = new NativeRowHydrator();

foreach ([...$conforming, ...$castOnly] as $label => [$s, $batch]) {
    printf(
        "%-9s hydrate:%s\n",
        $label,
        serialize($php->hydrate($batch, $s)) === serialize($native->hydrate($batch, $s)) ? 'yes' : 'NO',
    );
}

$resolved = static fn(mixed $value): string => get_debug_type(
    $native->hydrate([new RawRowValues(['id' => 1, 'a' => $value])], schema(int_schema('id'), new UnionDefinition('a', $union, true)))
        ->first()
        ->get('a'),
);

printf("int value type:%s\n", $resolved(42));
printf("string value type:%s\n", $resolved('x'));

$outside = [new RawRowValues(['a' => [1, 2]])];
$outsideSchema = schema(new UnionDefinition('a', $unmatchable));

$phpError = null;

try {
    $php->hydrate($outside, $outsideSchema);
} catch (Throwable $e) {
    $phpError = $e;
}

$nativeError = null;

try {
    $native->hydrate($outside, $outsideSchema);
} catch (Throwable $e) {
    $nativeError = $e;
}

printf(
    "outside member exception parity:%s\n",
    $phpError !== null && $nativeError !== null
        && $phpError::class === $nativeError::class
        && $phpError->getMessage() === $nativeError->getMessage() ? 'yes' : 'NO',
);
?>
--EXPECT--
members   hydrate:yes
metadata  hydrate:yes
castable  hydrate:yes
int value type:int
string value type:string
outside member exception parity:yes
