--TEST--
datetime ISO strings: both hydrators build the object new DateTimeImmutable($value) builds, the Z zone included
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\schema;

use Flow\ETL\Row\NativeRowHydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;

$values = [
    '2026-01-02T03:04:05Z',
    '2026-01-02 03:04:05Z',
    '2026-01-02T03:04Z',
    "2026-01-02T03:04:05Z\n",
    '2026-01-02T03:04:05.1Z',
    '2026-01-02T03:04:05.12Z',
    '2026-01-02T03:04:05.123Z',
    '2026-01-02T03:04:05.1234Z',
    '2026-01-02T03:04:05.12345Z',
    '2026-01-02T03:04:05.123456Z',
    '2026-01-02T03:04:05.1234567Z',
    '2026-01-02T03:04:05.12345678Z',
    '2026-01-02T03:04:05.123456789Z',
    '2026-12-31T23:59:60Z',
    '2026-01-02T24:00:00Z',
    '0001-01-01T00:00:00Z',
    '2026-01-02T03:04:05.123456789+02:00',
    '2026-01-02T03:04:05-0530',
    '2026-01-02T03:04:05-05',
    '2026-01-02T03:04:05+00:00',
    '2026-01-02 03:04',
    '2026-01-02T03:04:05',
    "2026-01-02T03:04\n",
    '2026-03-29T02:30:00',
];

$php = new PhpRowHydrator();
$native = new NativeRowHydrator();
$schema = schema(datetime_schema('at'));

foreach (['UTC', 'Europe/Warsaw'] as $timezone) {
    date_default_timezone_set($timezone);
    echo "date.timezone {$timezone}\n";

    foreach ($values as $value) {
        $batch = [new RawRowValues(['at' => $value])];
        $expected = new DateTimeImmutable($value);
        $phpAt = $php->hydrate($batch, $schema)->first()->get('at');
        $nativeAt = $native->hydrate($batch, $schema)->first()->get('at');

        printf(
            "%-38s %s %s type:%d php:%s native:%s\n",
            json_encode($value),
            $nativeAt->format('Y-m-d H:i:s.u'),
            $nativeAt->getTimezone()->getName(),
            json_decode(json_encode($nativeAt), true)['timezone_type'],
            serialize($phpAt) === serialize($expected) ? 'yes' : 'NO',
            serialize($nativeAt) === serialize($expected) ? 'yes' : 'NO',
        );
    }
}

foreach (['2026-01-02T25:99:99Z', '2026-01-02T03:60:00Z', '2026-01-02T03:04:05+25:00'] as $value) {
    $batch = [new RawRowValues(['at' => $value])];

    foreach (['php' => $php, 'native' => $native] as $label => $hydrator) {
        try {
            $hydrator->hydrate($batch, $schema);
            echo "{$label} {$value}: FAIL no exception\n";
        } catch (Throwable $e) {
            echo "{$label} {$value}: ", $e::class, "\n";
        }
    }
}
?>
--EXPECT--
date.timezone UTC
"2026-01-02T03:04:05Z"                 2026-01-02 03:04:05.000000 Z type:2 php:yes native:yes
"2026-01-02 03:04:05Z"                 2026-01-02 03:04:05.000000 Z type:2 php:yes native:yes
"2026-01-02T03:04Z"                    2026-01-02 03:04:00.000000 Z type:2 php:yes native:yes
"2026-01-02T03:04:05Z\n"               2026-01-02 03:04:05.000000 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.1Z"               2026-01-02 03:04:05.100000 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.12Z"              2026-01-02 03:04:05.120000 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.123Z"             2026-01-02 03:04:05.123000 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.1234Z"            2026-01-02 03:04:05.123400 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.12345Z"           2026-01-02 03:04:05.123450 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.123456Z"          2026-01-02 03:04:05.123456 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.1234567Z"         2026-01-02 03:04:05.123456 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.12345678Z"        2026-01-02 03:04:05.123456 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.123456789Z"       2026-01-02 03:04:05.123456 Z type:2 php:yes native:yes
"2026-12-31T23:59:60Z"                 2027-01-01 00:00:00.000000 Z type:2 php:yes native:yes
"2026-01-02T24:00:00Z"                 2026-01-03 00:00:00.000000 Z type:2 php:yes native:yes
"0001-01-01T00:00:00Z"                 0001-01-01 00:00:00.000000 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.123456789+02:00"  2026-01-02 03:04:05.123456 +02:00 type:1 php:yes native:yes
"2026-01-02T03:04:05-0530"             2026-01-02 03:04:05.000000 -05:30 type:1 php:yes native:yes
"2026-01-02T03:04:05-05"               2026-01-02 03:04:05.000000 -05:00 type:1 php:yes native:yes
"2026-01-02T03:04:05+00:00"            2026-01-02 03:04:05.000000 +00:00 type:1 php:yes native:yes
"2026-01-02 03:04"                     2026-01-02 03:04:00.000000 UTC type:3 php:yes native:yes
"2026-01-02T03:04:05"                  2026-01-02 03:04:05.000000 UTC type:3 php:yes native:yes
"2026-01-02T03:04\n"                   2026-01-02 03:04:00.000000 UTC type:3 php:yes native:yes
"2026-03-29T02:30:00"                  2026-03-29 02:30:00.000000 UTC type:3 php:yes native:yes
date.timezone Europe/Warsaw
"2026-01-02T03:04:05Z"                 2026-01-02 03:04:05.000000 Z type:2 php:yes native:yes
"2026-01-02 03:04:05Z"                 2026-01-02 03:04:05.000000 Z type:2 php:yes native:yes
"2026-01-02T03:04Z"                    2026-01-02 03:04:00.000000 Z type:2 php:yes native:yes
"2026-01-02T03:04:05Z\n"               2026-01-02 03:04:05.000000 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.1Z"               2026-01-02 03:04:05.100000 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.12Z"              2026-01-02 03:04:05.120000 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.123Z"             2026-01-02 03:04:05.123000 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.1234Z"            2026-01-02 03:04:05.123400 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.12345Z"           2026-01-02 03:04:05.123450 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.123456Z"          2026-01-02 03:04:05.123456 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.1234567Z"         2026-01-02 03:04:05.123456 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.12345678Z"        2026-01-02 03:04:05.123456 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.123456789Z"       2026-01-02 03:04:05.123456 Z type:2 php:yes native:yes
"2026-12-31T23:59:60Z"                 2027-01-01 00:00:00.000000 Z type:2 php:yes native:yes
"2026-01-02T24:00:00Z"                 2026-01-03 00:00:00.000000 Z type:2 php:yes native:yes
"0001-01-01T00:00:00Z"                 0001-01-01 00:00:00.000000 Z type:2 php:yes native:yes
"2026-01-02T03:04:05.123456789+02:00"  2026-01-02 03:04:05.123456 +02:00 type:1 php:yes native:yes
"2026-01-02T03:04:05-0530"             2026-01-02 03:04:05.000000 -05:30 type:1 php:yes native:yes
"2026-01-02T03:04:05-05"               2026-01-02 03:04:05.000000 -05:00 type:1 php:yes native:yes
"2026-01-02T03:04:05+00:00"            2026-01-02 03:04:05.000000 +00:00 type:1 php:yes native:yes
"2026-01-02 03:04"                     2026-01-02 03:04:00.000000 Europe/Warsaw type:3 php:yes native:yes
"2026-01-02T03:04:05"                  2026-01-02 03:04:05.000000 Europe/Warsaw type:3 php:yes native:yes
"2026-01-02T03:04\n"                   2026-01-02 03:04:00.000000 Europe/Warsaw type:3 php:yes native:yes
"2026-03-29T02:30:00"                  2026-03-29 03:30:00.000000 Europe/Warsaw type:3 php:yes native:yes
php 2026-01-02T25:99:99Z: Flow\ETL\Exception\SchemaMismatchException
native 2026-01-02T25:99:99Z: Flow\ETL\Exception\SchemaMismatchException
php 2026-01-02T03:60:00Z: Flow\ETL\Exception\SchemaMismatchException
native 2026-01-02T03:60:00Z: Flow\ETL\Exception\SchemaMismatchException
php 2026-01-02T03:04:05+25:00: Flow\ETL\Exception\SchemaMismatchException
native 2026-01-02T03:04:05+25:00: Flow\ETL\Exception\SchemaMismatchException
