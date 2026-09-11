--TEST--
datetime columns: a mutable input normalises to immutable, non-UTC timezone, microseconds
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

$rows = rows(
    schema(
        int_schema('id'),
        datetime_schema('immutable'),
        datetime_schema('mutable'),
        datetime_schema('nullable', nullable: true),
        datetime_schema('before_epoch'),
    ),
    row([
        'id' => 1,
        'immutable' => new DateTimeImmutable('2025-01-01 12:00:00.123456', new DateTimeZone('Europe/Warsaw')),
        // the class is not stored: a datetime column always hydrates to DateTimeImmutable, so a
        // DateTime written here comes back immutable, carrying the same instant and timezone
        'mutable' => new DateTime('1969-12-31 23:59:59.999999', new DateTimeZone('America/New_York')),
        'nullable' => null,
        // regression: timelib's trailing-data check reads the byte after the
        // parsed input - pre-1970 fractional epochs exposed a missing NUL
        // terminator in the extension's date construction
        'before_epoch' => new DateTimeImmutable('1969-07-20 20:17:00.5', new DateTimeZone('Europe/Warsaw')),
    ]),
);

$frames = php_frames($rows);
$actual = ext_decode_frames($frames);

assert_rows_identical(php_decode_frames($frames), $actual);

var_dump(get_class($actual[0]->get('immutable')));
var_dump($actual[0]->get('immutable')->format('Y-m-d H:i:s.u e'));
var_dump(get_class($actual[0]->get('mutable')));
var_dump($actual[0]->get('mutable')->format('Y-m-d H:i:s.u e'));
var_dump($actual[0]->get('nullable'));
var_dump($actual[0]->get('before_epoch')->format('Y-m-d H:i:s.u e'));
?>
--EXPECT--
identical
string(17) "DateTimeImmutable"
string(40) "2025-01-01 12:00:00.123456 Europe/Warsaw"
string(17) "DateTimeImmutable"
string(43) "1969-12-31 23:59:59.999999 America/New_York"
NULL
string(40) "1969-07-20 20:17:00.500000 Europe/Warsaw"
