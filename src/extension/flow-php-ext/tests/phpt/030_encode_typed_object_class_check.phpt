--TEST--
Typed uuid/json columns reject a foreign object instead of reading its memory layout
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Rows;
use Flow\Floe\RustFloeEncoderNative;

use function Flow\ETL\DSL\{row, schema, uuid_schema, json_schema, str_entry};

$encoder = new RustFloeEncoderNative();

// The value never matches the column type, so the writer assertion would normally stop it -
// this pins what the encoder itself does when reached with validateData: false. Without a
// class check read_slot() casts a raw property offset into a foreign object's layout.
foreach ([
    'uuid' => schema(uuid_schema('v')),
    'json' => schema(json_schema('v')),
] as $label => $columnSchema) {
    $badRow = row(str_entry('v', 'not-an-object-of-that-class'));
    $typed = (new PhpRowHydrator())->dehydrate(new Rows($badRow));

    expect_exception(fn() => $encoder->encode($typed, json_encode($columnSchema->normalize(), JSON_THROW_ON_ERROR)));
}

$wrongObject = row(new Flow\ETL\Row\Entry\DateTimeEntry('v', new DateTimeImmutable('2020-01-01 00:00:00 UTC')));
$typedObject = (new PhpRowHydrator())->dehydrate(new Rows($wrongObject));

expect_exception(fn() => $encoder->encode($typedObject, json_encode(schema(uuid_schema('v'))->normalize(), JSON_THROW_ON_ERROR)));
?>
--EXPECT--
Flow\Floe\Exception\ExtensionException: flow_php expected a uuid value to be an object
Flow\Floe\Exception\ExtensionException: flow_php expected a json value to be an object
Flow\Floe\Exception\ExtensionException: flow_php expected a uuid value to be an object
