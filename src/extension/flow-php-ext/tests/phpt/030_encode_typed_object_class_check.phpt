--TEST--
Typed uuid/json columns reject a foreign object instead of reading its memory layout
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\uuid_schema;

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Rows;
use Flow\Floe\RustFloeEncoderNative;

$encoder = new RustFloeEncoderNative();

// The value never matches the column type, so the writer assertion would normally stop it -
// this pins what the encoder itself does when reached with validateData: false. Without a
// class check read_slot() casts a raw property offset into a foreign object's layout.
foreach ([schema(uuid_schema('v')), schema(json_schema('v'))] as $columnSchema) {
    $typed = (new PhpRowHydrator())->dehydrate(new Rows($columnSchema, row(['v' => 'not-an-object-of-that-class'])));

    expect_exception(fn() => $encoder->encode($typed, json_encode($columnSchema->normalize(), JSON_THROW_ON_ERROR)));
}

$typedObject = (new PhpRowHydrator())->dehydrate(
    new Rows(schema(datetime_schema('v')), row(['v' => new DateTimeImmutable('2020-01-01 00:00:00 UTC')])),
);

expect_exception(fn() => $encoder->encode($typedObject, json_encode(schema(uuid_schema('v'))->normalize(), JSON_THROW_ON_ERROR)));
?>
--EXPECT--
Flow\Floe\Exception\ExtensionException: flow_php expected a uuid value to be an object
Flow\Floe\Exception\ExtensionException: flow_php expected a json value to be an object
Flow\Floe\Exception\ExtensionException: flow_php expected a uuid value to be an object
