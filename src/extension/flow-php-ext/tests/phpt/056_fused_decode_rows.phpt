--TEST--
RustFloeEncoderNative::decodeRows() yields the same Rows, and refuses with the same message, as hydrate(decode())
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

use Flow\ETL\Row\NativeRowHydrator;
use Flow\ETL\Row\RustRowHydratorNative;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\Floe\RustFloeEncoderNative;

/**
 * @return list<string>
 */
function bodies(Rows $rows, Schema $fileSchema): array
{
    $schemaBody = json_encode($fileSchema->normalize(), JSON_THROW_ON_ERROR);

    return (new RustFloeEncoderNative())->encode((new NativeRowHydrator())->dehydrate($rows), $schemaBody, $fileSchema);
}

function outcome(callable $read): string
{
    try {
        return serialize($read());
    } catch (Throwable $e) {
        $previous = $e->getPrevious();

        return get_class($e) . ': ' . $e->getMessage() . ($previous === null ? '' : ' <- ' . get_class($previous) . ': ' . $previous->getMessage());
    }
}

/**
 * @param list<string> $bodies
 */
function compare(string $label, array $bodies, string $schemaBody, Schema $schema): void
{
    $twoStep = outcome(static fn(): Rows => (new RustRowHydratorNative())->hydrate((new RustFloeEncoderNative())->decode($bodies, $schemaBody), $schema));
    $fused = outcome(static fn(): Rows => (new RustFloeEncoderNative())->decodeRows($bodies, $schemaBody, $schema));

    echo $label, ': ', $twoStep === $fused ? 'identical' : "FAIL\n  two-step: {$twoStep}\n  fused:    {$fused}", "\n";

    if (!str_starts_with($fused, 'O:')) {
        echo '  ', $fused, "\n";
    }
}

function compare_rows(string $label, Rows $rows, ?Schema $readSchema = null): void
{
    $fileSchema = $rows->schema();

    compare(
        $label,
        bodies($rows, $fileSchema),
        json_encode($fileSchema->normalize(), JSON_THROW_ON_ERROR),
        $readSchema ?? $fileSchema,
    );
}

compare_rows('017 rows', rows(
    schema(int_schema('id'), str_schema('name', nullable: true), float_schema('price', nullable: true), datetime_schema('at', nullable: true)),
    row(['id' => 1, 'name' => 'a']),
    row(['id' => 2, 'name' => null]),
    row(['id' => 3, 'price' => 1.5]),
    row(['id' => 4, 'at' => new DateTimeImmutable('2025-01-01 00:00:00.123456', new DateTimeZone('Europe/Warsaw'))]),
));

compare_rows('021 rows', rows(
    schema(int_schema('id'), str_schema('name', nullable: true), float_schema('price')),
    ...array_map(static fn(int $i) => row(['id' => $i, 'name' => $i % 7 === 0 ? null : 'user_' . $i, 'price' => $i / 4.0]), range(1, 500)),
));

$containers = rows(
    schema(
        int_schema('id'),
        datetime_schema('utc'),
        datetime_schema('zoned', nullable: true),
        list_schema('tags', type_list(type_string()), nullable: true),
        map_schema('scores', type_map(type_string(), type_integer())),
        structure_schema('address', type_structure(['street' => type_string(), 'zip' => type_integer()]), nullable: true),
        bool_schema('active'),
        int_schema('1', nullable: true),
    ),
    row(['id' => 1, 'utc' => new DateTimeImmutable('2026-03-01T10:11:12.654321Z'), 'zoned' => new DateTimeImmutable('2026-03-01 10:11:12', new DateTimeZone('+02:00')), 'tags' => ['a', 'b'], 'scores' => ['x' => 1], 'address' => ['street' => 'Main', 'zip' => 12345], 'active' => true, '1' => 7]),
    row(['id' => 2, 'utc' => new DateTimeImmutable('1970-01-01T00:00:00Z'), 'zoned' => null, 'tags' => null, 'scores' => [], 'address' => null, 'active' => false, '1' => null]),
    row(['id' => 3, 'utc' => new DateTimeImmutable('2026-12-31 23:59:59.000001', new DateTimeZone('Europe/Warsaw')), 'zoned' => new DateTimeImmutable('2026-06-01', new DateTimeZone('America/New_York')), 'tags' => [], 'scores' => ['y' => -2, 'z' => 3], 'address' => ['street' => '', 'zip' => 0], 'active' => true, '1' => 0]),
);

compare_rows('nullable / datetime / list / map / structure / numeric name', $containers);

$fileSchema = schema(int_schema('id'), str_schema('name', nullable: true), int_schema('7', nullable: true));
$tagged = rows(
    schema(int_schema('id'), str_schema('name', nullable: true, metadata: Metadata::fromArray(['source' => 'crm'])), int_schema('7', nullable: true, metadata: Metadata::fromArray(['n' => 1]))),
    row(['id' => 1, 'name' => 'a', '7' => 1]),
    row(['id' => 2, 'name' => null, '7' => null]),
);
compare('per-value metadata', bodies($tagged, $fileSchema), json_encode($fileSchema->normalize(), JSON_THROW_ON_ERROR), $fileSchema);

compare_rows('empty batch', rows(schema(int_schema('id'))));
compare('no frames', [], json_encode(schema(int_schema('id'))->normalize(), JSON_THROW_ON_ERROR), schema(int_schema('id')));

$narrow = rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => 'a']), row(['id' => 2, 'name' => 'b']));
compare_rows('read schema declares a nullable column the frames lack', $narrow, schema(int_schema('id'), str_schema('name'), str_schema('extra', nullable: true)));
compare_rows('read schema declares a NOT NULL column the frames lack', $narrow, schema(int_schema('id'), str_schema('name'), str_schema('extra')));
compare_rows('read schema drops a decoded column', $narrow, schema(int_schema('id')));
compare_rows('read schema reorders columns', $narrow, schema(str_schema('name'), int_schema('id')));
compare_rows('read schema refuses a decoded value', $narrow, schema(int_schema('id'), int_schema('name')));
compare_rows('read schema refuses a decoded null', rows(schema(int_schema('id'), str_schema('name', nullable: true)), row(['id' => 1, 'name' => null])), schema(int_schema('id'), str_schema('name')));

$narrowSchemaBody = json_encode($narrow->schema()->normalize(), JSON_THROW_ON_ERROR);
$narrowBodies = bodies($narrow, $narrow->schema());
compare('trailing bytes', [$narrowBodies[0] . "\xEF"], $narrowSchemaBody, $narrow->schema());
compare('truncated body', [substr($narrowBodies[1], 0, -1)], $narrowSchemaBody, $narrow->schema());
compare('unknown value flag', ["\x09" . substr($narrowBodies[0], 1)], $narrowSchemaBody, $narrow->schema());

$duplicatedSchemaBody = json_encode([...$narrow->schema()->normalize(), ...schema(int_schema('id'))->normalize()], JSON_THROW_ON_ERROR);
compare('duplicated decoded name', [$narrowBodies[0] . "\x01" . pack('q', 9)], $duplicatedSchemaBody, $narrow->schema());

// the two-step path decodes every body before it casts; the fused path casts each row as it decodes it - a cast
// refusal in row 1 is reported ahead of a corrupt frame in row 2 only by the fused path
$precedence = rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => '1']), row(['id' => 2, 'name' => 'x']));
$precedenceBody = json_encode($precedence->schema()->normalize(), JSON_THROW_ON_ERROR);
$precedenceBodies = bodies($precedence, $precedence->schema());
$precedenceInput = [$precedenceBodies[0], $precedenceBodies[1], $precedenceBodies[0] . "\xEF"];
$castsName = schema(int_schema('id'), int_schema('name'));
echo 'precedence two-step: ', outcome(static fn(): Rows => (new RustRowHydratorNative())->hydrate((new RustFloeEncoderNative())->decode($precedenceInput, $precedenceBody), $castsName)), "\n";
echo 'precedence fused:    ', outcome(static fn(): Rows => (new RustFloeEncoderNative())->decodeRows($precedenceInput, $precedenceBody, $castsName)), "\n";

$encoder = new RustFloeEncoderNative();
$first = $encoder->decodeRows($narrowBodies, $narrowSchemaBody, $narrow->schema());
$second = $encoder->decodeRows(bodies($containers, $containers->schema()), json_encode($containers->schema()->normalize(), JSON_THROW_ON_ERROR), $containers->schema());
$third = $encoder->decodeRows($narrowBodies, $narrowSchemaBody, $narrow->schema());
echo 'plans rebind across schemas: ', $first->count(), ' ', $second->count(), ' ', serialize($first) === serialize($third) ? 'identical' : 'FAIL', "\n";
?>
--EXPECT--
017 rows: identical
021 rows: identical
nullable / datetime / list / map / structure / numeric name: identical
per-value metadata: identical
empty batch: identical
no frames: identical
read schema declares a nullable column the frames lack: identical
read schema declares a NOT NULL column the frames lack: identical
  Flow\ETL\Exception\SchemaMismatchException: Rows do not match their schema: column "extra" (row 0) declared by the schema is missing from the row <- Flow\ETL\Exception\ColumnMismatchException: Row does not match its schema: column "extra" declared by the schema is missing from the row
read schema drops a decoded column: identical
read schema reorders columns: identical
read schema refuses a decoded value: identical
  Flow\ETL\Exception\SchemaMismatchException: Rows do not match their schema: column "name" (row 0): could not convert 'a' (string) to integer <- Flow\ETL\Exception\ColumnMismatchException: Row does not match its schema: column "name": could not convert 'a' (string) to integer
read schema refuses a decoded null: identical
  Flow\ETL\Exception\SchemaMismatchException: Rows do not match their schema: column "name" (row 0): could not convert null to string, column is not nullable <- Flow\ETL\Exception\ColumnMismatchException: Row does not match its schema: column "name": could not convert null to string, column is not nullable
trailing bytes: identical
  Flow\Floe\Exception\ExtensionException: flow_php row frame length does not match its content
truncated body: identical
  Flow\Floe\Exception\ExtensionException: flow_php frame body is truncated, string value is incomplete
unknown value flag: identical
  Flow\Floe\Exception\ExtensionException: flow_php found unknown value flag 0x09
duplicated decoded name: identical
  Flow\Floe\Exception\ExtensionException: flow_php found duplicated entry name "id" in a row frame
precedence two-step: Flow\Floe\Exception\ExtensionException: flow_php row frame length does not match its content
precedence fused:    Flow\ETL\Exception\SchemaMismatchException: Rows do not match their schema: column "name" (row 1): could not convert 'x' (string) to integer <- Flow\ETL\Exception\ColumnMismatchException: Row does not match its schema: column "name": could not convert 'x' (string) to integer
plans rebind across schemas: 2 3 identical
