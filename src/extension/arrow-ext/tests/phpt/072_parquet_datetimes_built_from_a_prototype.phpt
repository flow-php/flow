--TEST--
Datetimes and dates the arrow reader builds by cloning one prototype per zone behave as the PHP reader's own objects: calendar fields, timestamp, zone, modify(), diff(), setTimezone(), comparison and serialization, flat and inside a list, from far past to far future
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, NestedColumn};
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Context\ParquetRows;
use Flow\Parquet\Writer;

$state = 11;
$random = static function (int $modulo) use (&$state): int {
    $state = ($state * 1_103_515_245 + 12_345) % 2_147_483_648;

    return $state % $modulo;
};
$instants = [
    '@0', '@-1', '1969-12-31 23:59:59.999999 UTC', '1970-01-01 00:00:00.000001 UTC', '1900-02-28 23:59:59.5 UTC',
    '2000-02-29 23:59:59.999999 UTC', '2024-02-29 12:00:00 UTC', '1582-10-04 00:00:00 UTC', '0001-01-01 00:00:00 UTC',
    '-1000-06-15 06:30:00.25 UTC', '9999-12-31 23:59:59.999999 UTC', '2038-01-19 03:14:08 UTC',
];
$values = array_map(static fn(string $at): DateTimeImmutable => new DateTimeImmutable($at), $instants);
for ($i = 0; $i < 300; $i++) {
    $values[] = (new DateTimeImmutable('@' . (($random(2_000_000) - 1_000_000) * 100_003)))->modify('+' . $random(1_000_000) . ' microseconds');
}

$file = MemoryParquetFile::written(
    Writer::php(),
    Schema::with(FlatColumn::dateTime('ts'), FlatColumn::date('day'), NestedColumn::list('ts_list', ListElement::datetime())),
    array_map(static fn(DateTimeImmutable $at): array => ['ts' => $at, 'day' => $at, 'ts_list' => [$at, $at->modify('+1 day')]], $values),
);
$described = static function (DateTimeImmutable $at): string {
    return implode(' | ', [
        $at->format('Y-m-d H:i:s.u P T e U N z t L'),
        $at->getTimestamp(),
        $at->getOffset(),
        $at->modify('+1 month -3 hours')->format('Y-m-d H:i:s.u P'),
        $at->diff(new DateTimeImmutable('2001-02-03 04:05:06.789 UTC'))->format('%R %y %m %d %a %h %i %s %f'),
        $at->setTimezone(new DateTimeZone('Europe/Warsaw'))->format('Y-m-d H:i:s.u T'),
        $at <=> new DateTimeImmutable('@0'),
        serialize($at),
        json_encode($at),
    ]);
};
$read = static fn(Reader $reader): array => array_map(
    static fn(array $row): string => $described($row['ts']) . "\n" . $described($row['day']) . "\n" . $described($row['ts_list'][0]) . "\n" . $described($row['ts_list'][1]),
    ParquetRows::read(MemoryParquetFile::read($reader, $file), 100),
);
$php = $read(Reader::php());
$native = $read(new Reader(engine: new RustParquetEngine()));
$differ = array_keys(array_filter(array_map(static fn(string $a, string $b): bool => $a !== $b, $php, $native)));

echo count($php), ' rows, ', $differ === [] ? 'identical' : 'DIFFER at ' . implode(',', array_slice($differ, 0, 5)) . "\n" . $php[$differ[0]] . "\n---\n" . $native[$differ[0]], "\n";
echo $native[0], "\n";
?>
--EXPECT--
312 rows, identical
1970-01-01 00:00:00.000000 +00:00 GMT+0000 +00:00 0 4 0 31 0 | 0 | 0 | 1970-01-31 21:00:00.000000 +00:00 | + 31 1 2 11356 4 5 6 789000 | 1970-01-01 01:00:00.000000 CET | 0 | O:17:"DateTimeImmutable":3:{s:4:"date";s:26:"1970-01-01 00:00:00.000000";s:13:"timezone_type";i:1;s:8:"timezone";s:6:"+00:00";} | {"date":"1970-01-01 00:00:00.000000","timezone_type":1,"timezone":"+00:00"}
1970-01-01 00:00:00.000000 +00:00 UTC UTC 0 4 0 31 0 | 0 | 0 | 1970-01-31 21:00:00.000000 +00:00 | + 31 1 2 11356 4 5 6 789000 | 1970-01-01 01:00:00.000000 CET | 0 | O:17:"DateTimeImmutable":3:{s:4:"date";s:26:"1970-01-01 00:00:00.000000";s:13:"timezone_type";i:3;s:8:"timezone";s:3:"UTC";} | {"date":"1970-01-01 00:00:00.000000","timezone_type":3,"timezone":"UTC"}
1970-01-01 00:00:00.000000 +00:00 GMT+0000 +00:00 0 4 0 31 0 | 0 | 0 | 1970-01-31 21:00:00.000000 +00:00 | + 31 1 2 11356 4 5 6 789000 | 1970-01-01 01:00:00.000000 CET | 0 | O:17:"DateTimeImmutable":3:{s:4:"date";s:26:"1970-01-01 00:00:00.000000";s:13:"timezone_type";i:1;s:8:"timezone";s:6:"+00:00";} | {"date":"1970-01-01 00:00:00.000000","timezone_type":1,"timezone":"+00:00"}
1970-01-02 00:00:00.000000 +00:00 GMT+0000 +00:00 86400 5 1 31 0 | 86400 | 0 | 1970-02-01 21:00:00.000000 +00:00 | + 31 1 1 11355 4 5 6 789000 | 1970-01-02 01:00:00.000000 CET | 1 | O:17:"DateTimeImmutable":3:{s:4:"date";s:26:"1970-01-02 00:00:00.000000";s:13:"timezone_type";i:1;s:8:"timezone";s:6:"+00:00";} | {"date":"1970-01-02 00:00:00.000000","timezone_type":1,"timezone":"+00:00"}
