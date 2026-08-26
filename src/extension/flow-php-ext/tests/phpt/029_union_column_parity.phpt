--TEST--
NativeRowHydrator resolves a union column member per value, identically to PhpRowHydrator
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Row\NativeRowHydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema\Metadata;

use Flow\ETL\Schema\Definition\UnionDefinition;

use function Flow\ETL\DSL\{schema, int_schema};
use function Flow\Types\DSL\{type_datetime, type_integer, type_string, type_union, type_uuid};

$union = type_union(type_string(), type_integer());
$unmatchable = type_union(type_uuid(), type_datetime());

// hydrate() trusts the caller and never casts, so only member-conforming values belong here.
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

foreach ($conforming as $label => [$s, $batch]) {
    printf(
        "%-9s hydrate:%s cast:%s\n",
        $label,
        serialize($php->hydrate($batch, $s)) === serialize($native->hydrate($batch, $s)) ? 'yes' : 'NO',
        serialize($php->cast($batch, $s)) === serialize($native->cast($batch, $s)) ? 'yes' : 'NO',
    );
}

foreach ($castOnly as $label => [$s, $batch]) {
    printf(
        "%-9s cast:%s\n",
        $label,
        serialize($php->cast($batch, $s)) === serialize($native->cast($batch, $s)) ? 'yes' : 'NO',
    );
}

$entry = $native->cast([new RawRowValues(['id' => 1, 'a' => 42])], schema(int_schema('id'), new UnionDefinition('a', $union, true)))
    ->first()
    ->get('a');
printf("int value entry:%s\n", (new ReflectionClass($entry))->getShortName());

$entry = $native->cast([new RawRowValues(['id' => 1, 'a' => 'x'])], schema(int_schema('id'), new UnionDefinition('a', $union, true)))
    ->first()
    ->get('a');
printf("string value entry:%s\n", (new ReflectionClass($entry))->getShortName());

$outside = [new RawRowValues(['a' => [1, 2]])];
$outsideSchema = schema(new UnionDefinition('a', $unmatchable));

$phpError = null;

try {
    $php->cast($outside, $outsideSchema);
} catch (Throwable $e) {
    $phpError = $e;
}

$nativeError = null;

try {
    $native->cast($outside, $outsideSchema);
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
members   hydrate:yes cast:yes
metadata  hydrate:yes cast:yes
castable  cast:yes
int value entry:IntegerEntry
string value entry:StringEntry
outside member exception parity:yes
