--TEST--
RustFloeEncoderNative rejects DateTime subclasses with the Floe parity error
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Rows;
use Flow\Floe\RustFloeEncoderNative;

use function Flow\ETL\DSL\row;

class PhptCustomDateTime extends DateTimeImmutable
{
}

$badRow = row(new DateTimeEntry('custom', new PhptCustomDateTime('2020-05-05 10:20:30.000042', new DateTimeZone('Europe/Warsaw'))));
$typed = (new PhpRowHydrator())->dehydrate(new Rows($badRow));

$encoder = new RustFloeEncoderNative();

expect_exception(fn() => $encoder->encode($typed, json_encode($badRow->schema()->normalize(), JSON_THROW_ON_ERROR)));
?>
--EXPECT--
Flow\Floe\Exception\ExtensionException: Floe supports only DateTime and DateTimeImmutable, got PhptCustomDateTime - convert custom datetime instances before writing
