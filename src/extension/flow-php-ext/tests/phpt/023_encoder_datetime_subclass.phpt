--TEST--
RowsEncoder rejects DateTime subclasses with the Floe parity error
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\Floe\FloeWriter;
use Flow\Floe\RowsEncoder;

use function Flow\ETL\DSL\row;

class PhptCustomDateTime extends DateTimeImmutable
{
}

$badRow = row(new DateTimeEntry('custom', new PhptCustomDateTime('2020-05-05 10:20:30.000042', new DateTimeZone('Europe/Warsaw'))));

$encoder = new RowsEncoder();
$encoder->schema(FloeWriter::growSectionPlan(null, $badRow)->schemaBody);

expect_exception(fn() => $encoder->row($badRow));
?>
--EXPECT--
Flow\Floe\Exception\ExtensionException: Floe supports only DateTime and DateTimeImmutable, got PhptCustomDateTime - convert custom datetime instances before writing
