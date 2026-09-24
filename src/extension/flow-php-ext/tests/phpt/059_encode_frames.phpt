--TEST--
RustFloeEncoderNative::encodeFrames() writes the bytes, and refuses with the message, of framing encode(dehydrate())
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

use Flow\ETL\Row\RustRowHydratorNative;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\Floe\Format;
use Flow\Floe\RustFloeEncoderNative;

function outcome(callable $write): string
{
    try {
        return bin2hex($write());
    } catch (Throwable $e) {
        return get_class($e) . ': ' . $e->getMessage();
    }
}

function compare(string $label, Rows $rows, ?Schema $sessionSchema = null): void
{
    $sessionSchema ??= $rows->schema();
    $schemaBody = json_encode($sessionSchema->normalize(), JSON_THROW_ON_ERROR);

    $twoStep = outcome(static fn(): string => Format::rowFrames(
        (new RustFloeEncoderNative())->encode((new RustRowHydratorNative())->dehydrate($rows), $schemaBody, $sessionSchema),
    ));
    $fused = outcome(static fn(): string => (new RustFloeEncoderNative())->encodeFrames($rows, $schemaBody, $sessionSchema));

    echo $label, ': ', $twoStep === $fused ? 'identical' : "FAIL\n  two-step: {$twoStep}\n  fused:    {$fused}", "\n";

    if ($fused !== '' && !ctype_xdigit($fused)) {
        echo '  ', $fused, "\n";
    }
}

compare('017 rows', rows(
    schema(int_schema('id'), str_schema('name', nullable: true), float_schema('price', nullable: true), datetime_schema('at', nullable: true)),
    row(['id' => 1, 'name' => 'a']),
    row(['id' => 2, 'name' => null]),
    row(['id' => 3, 'price' => 1.5]),
    row(['id' => 4, 'at' => new DateTimeImmutable('2025-01-01 00:00:00.123456', new DateTimeZone('Europe/Warsaw'))]),
));

compare('021 rows', rows(
    schema(int_schema('id'), str_schema('name', nullable: true), float_schema('price')),
    ...array_map(static fn(int $i) => row(['id' => $i, 'name' => $i % 7 === 0 ? null : 'user_' . $i, 'price' => $i / 4.0]), range(1, 500)),
));

compare('034 nullable note', rows(
    schema(int_schema('id'), str_schema('name'), str_schema('note', nullable: true)),
    row(['id' => 1, 'name' => 'a', 'note' => null]),
));

compare('nullable / datetime / list / map / structure / numeric name', rows(
    schema(
        int_schema('id'),
        datetime_schema('zoned', nullable: true),
        list_schema('tags', type_list(type_string()), nullable: true),
        map_schema('scores', type_map(type_string(), type_integer())),
        structure_schema('address', type_structure(['street' => type_string(), 'zip' => type_integer()]), nullable: true),
        bool_schema('active'),
        int_schema('1', nullable: true),
    ),
    row(['id' => 1, 'zoned' => new DateTimeImmutable('2026-03-01 10:11:12', new DateTimeZone('+02:00')), 'tags' => ['a', 'b'], 'scores' => ['x' => 1], 'address' => ['street' => 'Main', 'zip' => 12345], 'active' => true, '1' => 7]),
    row(['id' => 2, 'zoned' => null, 'tags' => null, 'scores' => [], 'address' => null, 'active' => false, '1' => null]),
));

$sessionSchema = schema(int_schema('id'), str_schema('name', nullable: true), int_schema('7', nullable: true));
compare('batch metadata diverging from the session schema', rows(
    schema(int_schema('id'), str_schema('name', nullable: true, metadata: Metadata::fromArray(['source' => 'crm'])), int_schema('7', nullable: true, metadata: Metadata::fromArray(['n' => 1]))),
    row(['id' => 1, 'name' => 'a', '7' => 1]),
    row(['id' => 2, 'name' => null, '7' => null]),
), $sessionSchema);

compare('session schema carrying metadata', rows(
    schema(int_schema('id', metadata: Metadata::fromArray(['k' => 'v']))),
    row(['id' => 1]),
));

compare('empty Rows', rows(schema(int_schema('id'))));

$schema = schema(int_schema('id'), str_schema('name'));
compare('null under NOT NULL', Rows::trusted($schema, [row(['id' => 1, 'name' => 'a']), row(['id' => 2, 'name' => null])]));
compare('row without a declared column', Rows::trusted($schema, [row(['id' => 1])]));

// the two-step path dehydrates every row before it encodes; the fused path encodes each row as it reads it - a null
// under NOT NULL in row 1 is reported ahead of a missing column in row 2 only by the fused path
$precedence = Rows::trusted($schema, [row(['id' => 1, 'name' => 'a']), row(['id' => 2, 'name' => null]), row(['id' => 3])]);
$precedenceBody = json_encode($schema->normalize(), JSON_THROW_ON_ERROR);
echo 'precedence two-step: ', outcome(static fn(): string => Format::rowFrames((new RustFloeEncoderNative())->encode((new RustRowHydratorNative())->dehydrate($precedence), $precedenceBody, $schema))), "\n";
echo 'precedence fused:    ', outcome(static fn(): string => (new RustFloeEncoderNative())->encodeFrames($precedence, $precedenceBody, $schema)), "\n";

$encoder = new RustFloeEncoderNative();
$small = rows($schema, row(['id' => 1, 'name' => 'a']));
$first = $encoder->encodeFrames($small, json_encode($schema->normalize(), JSON_THROW_ON_ERROR), $schema);
$other = schema(int_schema('id'));
$encoder->encodeFrames(rows($other, row(['id' => 9])), json_encode($other->normalize(), JSON_THROW_ON_ERROR), $other);
echo 'plans rebind across schemas: ', $first === $encoder->encodeFrames($small, json_encode($schema->normalize(), JSON_THROW_ON_ERROR), $schema) ? 'identical' : 'FAIL', "\n";
?>
--EXPECT--
017 rows: identical
021 rows: identical
034 nullable note: identical
nullable / datetime / list / map / structure / numeric name: identical
batch metadata diverging from the session schema: identical
session schema carrying metadata: identical
empty Rows: identical
null under NOT NULL: identical
  Flow\ETL\Exception\SchemaMismatchException: Rows do not match their schema: column "name" (row 1): could not convert null to string, column is not nullable
row without a declared column: identical
  Flow\Floe\Exception\ExtensionException: flow_php found a row that does not carry the declared column "name"
precedence two-step: Flow\Floe\Exception\ExtensionException: flow_php found a row that does not carry the declared column "name"
precedence fused:    Flow\ETL\Exception\SchemaMismatchException: Rows do not match their schema: column "name" (row 1): could not convert null to string, column is not nullable
plans rebind across schemas: identical
