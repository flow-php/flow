--TEST--
datetime columns: immutable + mutable classes, non-UTC timezone, microseconds
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\Floe\RowsDecoder;

use function Flow\ETL\DSL\{row, rows, int_entry, datetime_entry};

$rows = rows(
    row(
        int_entry('id', 1),
        datetime_entry('immutable', new DateTimeImmutable('2025-01-01 12:00:00.123456', new DateTimeZone('Europe/Warsaw'))),
        new DateTimeEntry('mutable', new DateTime('1969-12-31 23:59:59.999999', new DateTimeZone('America/New_York'))),
        datetime_entry('nullable', null),
        // regression: timelib's trailing-data check reads the byte after the
        // parsed input - pre-1970 fractional epochs exposed a missing NUL
        // terminator in the extension's date construction
        datetime_entry('before_epoch', new DateTimeImmutable('1969-07-20 20:17:00.5', new DateTimeZone('Europe/Warsaw'))),
    ),
);

$frames = php_frames($rows);
$actual = decoder_decode_frames(new RowsDecoder(), $frames);

assert_rows_identical(php_decode_frames($frames), $actual);

var_dump(get_class($actual[0]->get('immutable')->value()));
var_dump($actual[0]->get('immutable')->value()->format('Y-m-d H:i:s.u e'));
var_dump(get_class($actual[0]->get('mutable')->value()));
var_dump($actual[0]->get('mutable')->value()->format('Y-m-d H:i:s.u e'));
var_dump($actual[0]->get('nullable')->value());
var_dump($actual[0]->get('before_epoch')->value()->format('Y-m-d H:i:s.u e'));
?>
--EXPECT--
identical
string(17) "DateTimeImmutable"
string(40) "2025-01-01 12:00:00.123456 Europe/Warsaw"
string(8) "DateTime"
string(43) "1969-12-31 23:59:59.999999 America/New_York"
NULL
string(40) "1969-07-20 20:17:00.500000 Europe/Warsaw"
