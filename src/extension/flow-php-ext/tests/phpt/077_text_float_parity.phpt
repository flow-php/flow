--TEST--
The native CSV and JSON writers write a float as the PHP writers do: 100 000 doubles from random bits plus the edge literals, under serialize_precision -1 and 17
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\PhpCSVEncoder;
use Flow\ETL\Adapter\CSV\CSVWriteOptions;
use Flow\ETL\Adapter\CSV\RustCSVEncoder;
use Flow\ETL\Adapter\JSON\PhpJSONEncoder;
use Flow\ETL\Adapter\JSON\RustJSONEncoder;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\schema;

mt_srand(77);

$floats = [1.0, -0.0, 0.0, 1.0e25, 1.0e-7, PHP_FLOAT_MAX, -PHP_FLOAT_MAX, PHP_FLOAT_MIN, 5.0e-324, 4.9e-324, 0.1 + 0.2, 1 / 3, 123456789012345.67, 1.0e15, 1.0e16, 1.0e17, 0.0001, 0.00001, 100.0, -1.5];

while (count($floats) < 100_020) {
    $floats[] = unpack('e', pack('V2', mt_rand(0, 0xffffffff), mt_rand(0, 0xffffffff)))[1];
}

$finite = array_values(array_filter($floats, is_finite(...)));
$schema = schema(float_schema('f'));
$options = new CSVWriteOptions(newLineSeparator: "\n");
$csv = new RustCSVEncoder(',', '"', '\\', "\n", DATE_ATOM, 'Y-m-d', new PhpCSVEncoder($options));
$rows = static fn(callable $build, array $values): array => array_map(
    static fn(array $chunk): Flow\ETL\Rows => $build($schema, array_map(static fn(float $f): array => ['f' => $f], $chunk)),
    array_chunk($values, 10_000),
);

foreach (['-1', '17'] as $precision) {
    ini_set('serialize_precision', $precision);
    ini_set('precision', $precision === '17' ? '5' : '14');

    $php = implode('', array_map((new PhpCSVEncoder($options))->encode(...), $rows(php_rows(...), $floats)));
    $native = implode('', array_map(static fn(Flow\ETL\Rows $batch): string => $csv->encode($batch), $rows(native_rows(...), $floats)));
    printf("serialize_precision %s: csv %s, %d values\n", $precision, $php === $native ? 'identical' : 'DIFFERENT', substr_count($native, "\n"));

    foreach ([JSON_THROW_ON_ERROR, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION] as $flags) {
        $json = new RustJSONEncoder($flags, DATE_ATOM, 'Y-m-d', new PhpJSONEncoder($flags));
        $php = implode("\n", array_map(static fn(Flow\ETL\Rows $batch): string => (new PhpJSONEncoder($flags))->encode($batch, "\n"), $rows(php_rows(...), $finite)));
        $native = implode("\n", array_map(static fn(Flow\ETL\Rows $batch): string => $json->encode($batch, "\n"), $rows(native_rows(...), $finite)));
        printf("serialize_precision %s: json flags %d %s, %d values\n", $precision, $flags, $php === $native ? 'identical' : 'DIFFERENT', substr_count($native, "\n") + 1);
    }

    foreach ([NAN, INF, -INF] as $float) {
        $values = [['f' => 1.5], ['f' => $float]];
        $php = written(static fn() => (new PhpJSONEncoder())->encode(php_rows($schema, $values), "\n"));
        $native = written(static fn() => (new RustJSONEncoder(JSON_THROW_ON_ERROR, DATE_ATOM, 'Y-m-d', new PhpJSONEncoder()))->encode(native_rows($schema, $values), "\n"));
        printf("json %s: %s\n", var_export($float, true), $php === $native ? $php : 'DIFFERENT');
    }

    var_dump(ini_get('serialize_precision') === $precision);
}
?>
--EXPECT--
serialize_precision -1: csv identical, 100020 values
serialize_precision -1: json flags 4194304 identical, 99973 values
serialize_precision -1: json flags 4195328 identical, 99973 values
json NAN: Flow\ETL\Exception\RuntimeException: Failed to encode JSON: Inf and NaN cannot be JSON encoded
json INF: Flow\ETL\Exception\RuntimeException: Failed to encode JSON: Inf and NaN cannot be JSON encoded
json -INF: Flow\ETL\Exception\RuntimeException: Failed to encode JSON: Inf and NaN cannot be JSON encoded
bool(true)
serialize_precision 17: csv identical, 100020 values
serialize_precision 17: json flags 4194304 identical, 99973 values
serialize_precision 17: json flags 4195328 identical, 99973 values
json NAN: Flow\ETL\Exception\RuntimeException: Failed to encode JSON: Inf and NaN cannot be JSON encoded
json INF: Flow\ETL\Exception\RuntimeException: Failed to encode JSON: Inf and NaN cannot be JSON encoded
json -INF: Flow\ETL\Exception\RuntimeException: Failed to encode JSON: Inf and NaN cannot be JSON encoded
bool(true)
