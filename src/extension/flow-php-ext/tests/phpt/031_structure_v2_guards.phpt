--TEST--
structure schema-shape skew guards: the legacy two-bucket shape and an empty fields list are rejected loudly
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Rows;
use Flow\Floe\RustFloeEncoderNative;

$schema = schema(int_schema('id'));
$typed = (new PhpRowHydrator())->dehydrate(new Rows($schema, row(['id' => 1])));

// direction 2 of the skew guard: a new binary reading the legacy two-bucket shape
// has no "structure" arm and rejects the unknown tag.
$legacy = json_encode([[
    'ref' => 'st',
    'type' => [
        'type' => 'structure',
        'elements' => ['a' => ['type' => 'integer']],
        'optional_elements' => [],
        'allow_extra' => false,
    ],
    'nullable' => false,
    'metadata' => [],
]], JSON_THROW_ON_ERROR);

expect_exception(fn() => (new RustFloeEncoderNative())->encode($typed, $legacy, $schema));

// the belt: a structure_v2 whose fields deserialized empty for any other reason.
$emptyFields = json_encode([[
    'ref' => 'st',
    'type' => [
        'type' => 'structure_v2',
        'fields' => [],
        'allow_extra' => false,
    ],
    'nullable' => false,
    'metadata' => [],
]], JSON_THROW_ON_ERROR);

expect_exception(fn() => (new RustFloeEncoderNative())->encode($typed, $emptyFields, $schema));
?>
--EXPECT--
Flow\Floe\Exception\ExtensionException: flow_php does not support values of type "structure" in this build
Flow\Floe\Exception\ExtensionException: flow_php read a structure type with no fields; the loaded flow_php extension and the flow-php/etl in use disagree on the structure schema format - reinstall one to match the other, or set the Floe engine to FloeEngine::php
