--TEST--
CSVOpenSink writes the same bytes through NativeCSVEncoder and PhpCSVEncoder: seeded random batches over every kind, nesting <= 3, random options
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\CSVOpenSink;
use Flow\ETL\Adapter\CSV\CSVWriteOptions;
use Flow\ETL\Adapter\CSV\NativeCSVEncoder;
use Flow\ETL\Adapter\CSV\NativeCSVWriter;
use Flow\ETL\Adapter\CSV\PhpCSVEncoder;
use Flow\Filesystem\DestinationStream;

mt_srand(80);

$separators = [',', ',', ';', "\t", '|', '.', '-', 'e', ' '];
$enclosures = ['"', '"', "'"];
$escapes = ['\\', '\\', '', '"', '/'];
$eols = ["\n", "\r\n"];
$dateTimeFormats = [DATE_ATOM, DATE_ATOM, 'Y-m-d H:i:s.u', 'U.v e T', 'D, d M Y H:i', 'c', 'y n j G p O Z', '\a\t Y/m/d'];
$dateFormats = ['Y-m-d', 'Y-m-d', 'd/m/Y', 'jS F Y', 'Ymd'];
$identical = 0;
$different = 0;
$refused = 0;
$unrendered = 0;

for ($i = 0; $i < 2_000; $i++) {
    [$schema, $rows] = write_random_batch();
    $options = new CSVWriteOptions(
        $separators[mt_rand(0, count($separators) - 1)],
        $enclosures[mt_rand(0, count($enclosures) - 1)],
        $escapes[mt_rand(0, count($escapes) - 1)],
        $eols[mt_rand(0, count($eols) - 1)],
        $dateTimeFormats[mt_rand(0, count($dateTimeFormats) - 1)],
        $dateFormats[mt_rand(0, count($dateFormats) - 1)],
    );

    if ($options->separator === $options->enclosure) {
        continue;
    }

    $header = mt_rand(0, 1) === 1;
    $writer = new NativeCSVWriter($options->separator, $options->enclosure, $options->escape, $options->newLineSeparator, $options->dateTimeFormat, $options->dateFormat);
    $unrendered += count($writer->unrendered($schema));
    // a second batch proves the header is written once and the encoders hold no state across batches
    $half = intdiv(count($rows), 2);

    $php = written(static function (DestinationStream $stream) use ($schema, $rows, $options, $header, $half): void {
        $sink = new CSVOpenSink($stream, new PhpCSVEncoder($options), $header);
        $sink->write(php_rows($schema, array_slice($rows, 0, $half)));
        $sink->write(php_rows($schema, array_slice($rows, $half)));
    });
    $native = written(static function (DestinationStream $stream) use ($schema, $rows, $options, $header, $half, $writer): void {
        $sink = new CSVOpenSink($stream, new NativeCSVEncoder($writer, new PhpCSVEncoder($options)), $header);
        $sink->write(native_rows($schema, array_slice($rows, 0, $half)));
        // a PHP column inside the batch is adopted
        $sink->write(php_rows($schema, array_slice($rows, $half)));
    });

    $refused += (int) refused($php);

    if ($php === $native) {
        $identical++;
    } elseif (++$different <= 3) {
        echo "batch {$i}: ", json_encode(array_map(static fn($d) => $d->type()->toString(), $schema->definitions())), ' ', json_encode([$options->separator, $options->enclosure, $options->escape, $options->dateTimeFormat, $options->dateFormat]), "\n  php:    ", substr(json_encode($php, JSON_INVALID_UTF8_SUBSTITUTE), 0, 600), "\n  native: ", substr(json_encode($native, JSON_INVALID_UTF8_SUBSTITUTE), 0, 600), "\n";
    }
}

echo "{$different} of " . ($identical + $different) . " batches differ\n";
echo $refused === 0 && $unrendered > 0 ? "every batch was written, some columns through PHP cells\n" : "{$refused} refusals, {$unrendered} columns left to PHP\n";
?>
--EXPECT--
0 of 2000 batches differ
every batch was written, some columns through PHP cells
