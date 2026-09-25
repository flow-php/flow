--TEST--
datetime column zones: the capability marker, and a zone-less datetime from an older flow-php/etl keeps each stored zone
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_datetime;

use Flow\ETL\Row\RustRowHydratorNative;
use Flow\ETL\Row\TypedRowValues;
use Flow\Floe\PhpFloeEncoder;
use Flow\Floe\RustFloeEncoderNative;

var_dump((new RustFloeEncoderNative())->datetimeZones());
var_dump((new RustRowHydratorNative())->datetimeZones());

$bodies = (new PhpFloeEncoder(schema(datetime_schema('at'))))->encode([
    new TypedRowValues(['at' => new DateTimeImmutable('2026-01-02 03:04:05', new DateTimeZone('Europe/Warsaw'))], ['at' => type_datetime()]),
]);
$zoneless = '[{"ref":"at","type":{"type":"datetime"},"nullable":false,"metadata":[]}]';
$zoned = json_encode(schema(datetime_schema('at'))->normalize(), JSON_THROW_ON_ERROR);

echo 'zone-less datetime: ', (new RustFloeEncoderNative())->decode($bodies, $zoneless)[0]->values['at']->format('Y-m-d H:i:s e'), "\n";
echo 'datetime<UTC>: ', (new RustFloeEncoderNative())->decode($bodies, $zoned)[0]->values['at']->format('Y-m-d H:i:s e'), "\n";
?>
--EXPECT--
bool(true)
bool(true)
zone-less datetime: 2026-01-02 03:04:05 Europe/Warsaw
datetime<UTC>: 2026-01-02 02:04:05 UTC
