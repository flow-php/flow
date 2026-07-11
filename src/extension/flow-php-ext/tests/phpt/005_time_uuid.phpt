--TEST--
time (DateInterval) and uuid columns round-trip identically
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Floe\RowsDecoder;

use function Flow\ETL\DSL\{row, rows, int_entry, time_entry, uuid_entry};

$negative = (new DateTimeImmutable('2025-01-02'))->diff(new DateTimeImmutable('2025-01-01'));
$fractional = new DateInterval('PT1S');
$fractional->f = 0.123456;

$rows = rows(
    row(
        int_entry('id', 1),
        time_entry('duration', new DateInterval('P3DT4H5M6S')),
        time_entry('negative', $negative),
        time_entry('fractional', $fractional),
        uuid_entry('uuid', '01234567-89ab-4def-8123-456789abcdef'),
    ),
);

$frames = php_frames($rows);
$actual = decoder_decode_frames(new RowsDecoder(), $frames);

assert_rows_identical(php_decode_frames($frames), $actual);

$duration = $actual[0]->get('duration')->value();
var_dump($duration->d, $duration->h, $duration->i, $duration->s);
var_dump($actual[0]->get('negative')->value()->invert);
var_dump($actual[0]->get('fractional')->value()->f);
var_dump((string) $actual[0]->get('uuid')->value());
?>
--EXPECT--
identical
int(3)
int(4)
int(5)
int(6)
int(1)
float(0.123456)
string(36) "01234567-89ab-4def-8123-456789abcdef"
