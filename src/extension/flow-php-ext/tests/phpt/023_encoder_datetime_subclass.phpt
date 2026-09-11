--TEST--
RustFloeEncoderNative rejects DateTime subclasses with the Floe parity error
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Rows;
use Flow\Floe\RustFloeEncoderNative;

class PhptCustomDateTime extends DateTimeImmutable
{
}

$schema = schema(datetime_schema('custom'));
$typed = (new PhpRowHydrator())->dehydrate(
    new Rows($schema, row(['custom' => new PhptCustomDateTime('2020-05-05 10:20:30.000042', new DateTimeZone('Europe/Warsaw'))])),
);

$encoder = new RustFloeEncoderNative();

expect_exception(fn() => $encoder->encode($typed, json_encode($schema->normalize(), JSON_THROW_ON_ERROR), $schema));
?>
--EXPECT--
Flow\Floe\Exception\ExtensionException: Floe supports only DateTime and DateTimeImmutable, got PhptCustomDateTime - convert custom datetime instances before writing
