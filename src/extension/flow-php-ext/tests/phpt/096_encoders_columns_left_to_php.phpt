--TEST--
RustCSVEncoder / RustJSONEncoder render natively what they can and leave the rest, by column, to the held PHP encoder's cells() / fragments()
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\{CSVWriteOptions, PhpCSVEncoder, RustCSVEncoder};
use Flow\ETL\Adapter\JSON\{PhpJSONEncoder, RustJSONEncoder};
use Flow\ETL\Tests\Context\TextWriterBatches;

$cases = [
    ['scalars with nulls', 'Y-m-d'],
    ['temporal kinds', 'Y-m-d\TH:i:sP'],
    ['temporal kinds', 'D, d M Y'],
    ['a json column', 'Y-m-d'],
    ['markup columns', 'Y-m-d'],
    ['nested kinds rendered natively', 'Y-m-d'],
    ['nested kinds rendered natively', 'l'],
    ['a list of json documents', 'Y-m-d'],
    ['nested markup leaves', 'Y-m-d'],
];

foreach ($cases as [$batch, $format]) {
    [$schema, $values] = TextWriterBatches::of($batch);
    $rows = native_rows($schema, $values);
    $csv = new RecordingPhpCSVEncoder(new PhpCSVEncoder(new CSVWriteOptions(dateTimeFormat: $format, dateFormat: $format)));
    $json = new RecordingPhpJSONEncoder(new PhpJSONEncoder(JSON_THROW_ON_ERROR, $format, $format));
    (new RustCSVEncoder(',', '"', '\\', "\n", $format, $format, $csv))->encode($rows);
    (new RustJSONEncoder(JSON_THROW_ON_ERROR, $format, $format, $json))->encode($rows, "\n");

    echo "{$batch} ({$format}): csv ", json_encode(recorded_names($csv->columns, $rows)), ', json ', json_encode(recorded_names($json->columns, $rows)), "\n";
}
?>
--EXPECT--
scalars with nulls (Y-m-d): csv [], json []
temporal kinds (Y-m-d\TH:i:sP): csv [], json []
temporal kinds (D, d M Y): csv ["at","offset","on"], json ["at","offset","on"]
a json column (Y-m-d): csv [], json ["j"]
markup columns (Y-m-d): csv ["x","e"], json ["x","e"]
nested kinds rendered natively (Y-m-d): csv [], json []
nested kinds rendered natively (l): csv ["l","s"], json ["l","s"]
a list of json documents (Y-m-d): csv ["l"], json ["l"]
nested markup leaves (Y-m-d): csv ["l","s"], json ["l","s"]
