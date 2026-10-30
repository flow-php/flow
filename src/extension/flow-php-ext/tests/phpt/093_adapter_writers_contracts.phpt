--TEST--
RustCSVEncoder / RustJsonEncoder implement CSVEncoder / JsonEncoder: unrendered columns through the held PHP encoder's cells() / fragments(), the cache refreshed on a new Schema, the PHP header; RustParquetFileWriter implements FileWriter
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\IO\Loader\CSVEncoder;
use Flow\ETL\Adapter\CSV\IO\Loader\CSVWriteOptions;
use Flow\ETL\Adapter\CSV\IO\Loader\RustCSVEncoder;
use Flow\ETL\Adapter\CSV\IO\Loader\PhpCSVEncoder;
use Flow\ETL\Adapter\JSON\IO\Loader\JsonEncoder;
use Flow\ETL\Adapter\JSON\IO\Loader\RustJsonEncoder;
use Flow\ETL\Adapter\JSON\IO\Loader\PhpJsonEncoder;
use Flow\ETL\Adapter\Parquet\IO\Loader\RustParquetFileWriter;
use Flow\ETL\IO\Loader\File\FileWriter;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\xml_schema;

$csvPhp = new PhpCSVEncoder(new CSVWriteOptions(newLineSeparator: "\n"));
$csv = new RustCSVEncoder(new CSVWriteOptions(',', '"', '\\', "\n", DATE_ATOM, 'Y-m-d'), $csvPhp);
$jsonPhp = new PhpJsonEncoder();
$json = new RustJsonEncoder($jsonPhp);

var_dump($csv instanceof CSVEncoder, $json instanceof JsonEncoder, (new ReflectionClass(RustParquetFileWriter::class))->implementsInterface(FileWriter::class));

$plain = schema(int_schema('id'));
$values = [['id' => 1, 'doc' => '<a>1</a>'], ['id' => 2, 'doc' => '<b/>']];
$xml = schema(int_schema('id'), xml_schema('doc'));
$jsonDoc = schema(int_schema('id'), json_schema('doc'));
$jsonValues = [['id' => 1, 'doc' => '{"a":1}'], ['id' => 2, 'doc' => '[1,2]']];

$csvRecorder = new RecordingPhpCSVEncoder($csvPhp);
$jsonRecorder = new RecordingPhpJsonEncoder($jsonPhp);
$xmlRows = native_rows($xml, $values);
$jsonRows = native_rows($jsonDoc, $jsonValues);
(new RustCSVEncoder(new CSVWriteOptions(',', '"', '\\', "\n", DATE_ATOM, 'Y-m-d'), $csvRecorder))->encode($xmlRows);
(new RustJsonEncoder($jsonRecorder))->encode($jsonRows, "\n");
echo json_encode([recorded_names($csvRecorder->columns, $xmlRows), recorded_names($jsonRecorder->columns, $jsonRows)]), "\n";

// the first batch caches a Schema without unrendered columns; a new Schema object must refresh it
foreach ([[$plain, [['id' => 1]]], [$xml, $values]] as [$schema, $batch]) {
    echo $csv->encode(native_rows($schema, $batch)) === $csvPhp->encode(php_rows($schema, $batch)) ? 'csv identical' : 'csv DIFFERENT', "\n";
}

foreach ([[$plain, [['id' => 1]]], [$jsonDoc, $jsonValues]] as [$schema, $batch]) {
    echo $json->encode(native_rows($schema, $batch), "\n") === $jsonPhp->encode(php_rows($schema, $batch), "\n") ? 'json identical' : 'json DIFFERENT', "\n";
}

echo $csv->encodeHeader(['id', 'a "b"', 'c,d']) === $csvPhp->encodeHeader(['id', 'a "b"', 'c,d']) ? 'header identical' : 'header DIFFERENT', "\n";
echo $csv->encode(native_rows($xml, $values));
echo $json->encode(native_rows($jsonDoc, $jsonValues), "\n"), "\n";
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
[["doc"],["doc"]]
csv identical
csv identical
json identical
json identical
header identical
1,<a>1</a>
2,<b/>
{"id":1,"doc":{"a":1}}
{"id":2,"doc":[1,2]}
