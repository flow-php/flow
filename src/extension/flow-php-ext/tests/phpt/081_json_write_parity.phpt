--TEST--
JsonOpenSink writes the same bytes through RustJSONEncoder and PhpJSONEncoder: seeded random batches over every kind, nesting <= 3, random flags and framings
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\JSON\JsonFraming;
use Flow\ETL\Adapter\JSON\JsonOpenSink;
use Flow\ETL\Adapter\JSON\RustJSONEncoder;
use Flow\ETL\Adapter\JSON\PhpJSONEncoder;
use Flow\Filesystem\DestinationStream;

mt_srand(81);

$dateTimeFormats = [DATE_ATOM, DATE_ATOM, 'Y-m-d H:i:s.u', 'U.v e T', 'D, d M Y H:i', 'c', 'y n j G p O Z', '\a\t Y/m/d "e"'];
$dateFormats = ['Y-m-d', 'Y-m-d', 'd/m/Y', 'jS F Y', 'Ymd'];
$identical = 0;
$different = 0;
$refused = 0;
$unrendered = 0;

for ($i = 0; $i < 2_000; $i++) {
    [$schema, $rows] = write_random_batch();
    $flags = (mt_rand(0, 3) > 0 ? JSON_THROW_ON_ERROR : 0)
        | (mt_rand(0, 1) ? JSON_UNESCAPED_SLASHES : 0)
        | (mt_rand(0, 1) ? JSON_UNESCAPED_UNICODE : 0)
        | (mt_rand(0, 1) ? JSON_PRESERVE_ZERO_FRACTION : 0);
    $framing = JsonFraming::cases()[mt_rand(0, 2)];
    $dateTimeFormat = $dateTimeFormats[mt_rand(0, count($dateTimeFormats) - 1)];
    $dateFormat = $dateFormats[mt_rand(0, count($dateFormats) - 1)];
    $recorder = new RecordingPhpJSONEncoder(new PhpJSONEncoder($flags, $dateTimeFormat, $dateFormat));
    $writer = new RustJSONEncoder($flags, $dateTimeFormat, $dateFormat, $recorder);
    $half = intdiv(count($rows), 2);

    $php = written(static function (DestinationStream $stream) use ($schema, $rows, $flags, $dateTimeFormat, $dateFormat, $framing, $half): void {
        $sink = new JsonOpenSink($stream, new PhpJSONEncoder($flags, $dateTimeFormat, $dateFormat), $framing);
        $sink->write(php_rows($schema, array_slice($rows, 0, $half)));
        $sink->write(php_rows($schema, array_slice($rows, $half)));
        $sink->close();
    });
    $native = written(static function (DestinationStream $stream) use ($schema, $rows, $flags, $dateTimeFormat, $dateFormat, $framing, $half, $writer): void {
        $sink = new JsonOpenSink($stream, $writer, $framing);
        $sink->write(native_rows($schema, array_slice($rows, 0, $half)));
        // a PHP column inside the batch is adopted
        $sink->write(php_rows($schema, array_slice($rows, $half)));
        $sink->close();
    });

    $refused += (int) refused($php);
    $unrendered += count($recorder->columns);

    if ($php === $native) {
        $identical++;
    } elseif (++$different <= 3) {
        echo "batch {$i}: ", json_encode(array_map(static fn($d) => $d->type()->toString(), $schema->definitions())), ' ', json_encode([$flags, $framing->name, $dateTimeFormat, $dateFormat]), "\n  php:    ", substr(json_encode($php, JSON_INVALID_UTF8_SUBSTITUTE), 0, 600), "\n  native: ", substr(json_encode($native, JSON_INVALID_UTF8_SUBSTITUTE), 0, 600), "\n";
    }
}

echo "{$different} of " . ($identical + $different) . " batches differ\n";
echo $refused === 0 && $unrendered > 0 ? "every batch was written, some columns through PHP fragments\n" : "{$refused} refusals, {$unrendered} columns left to PHP\n";
?>
--EXPECT--
0 of 2000 batches differ
every batch was written, some columns through PHP fragments
