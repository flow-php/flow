--TEST--
ISO date strings: both hydrators read them in the column zone - midnight UTC for date, the declared zone for datetime - whatever date.timezone says
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\schema;

use Flow\ETL\Row\NativeRowHydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;

$values = [
    '2026-01-02',
    '2024-02-29',
    "2026-01-02\n",
    '0001-01-01',
    '9999-12-31',
    // a DST gap at midnight in America/Santiago: the constructor lands on 01:00
    '2026-09-06',
];

$php = new PhpRowHydrator();
$native = new NativeRowHydrator();
$schema = schema(date_schema('on'), datetime_schema('at'), datetime_schema('at_scl', zone: 'America/Santiago'));

foreach (['UTC', 'Europe/Warsaw', 'America/Santiago'] as $timezone) {
    date_default_timezone_set($timezone);
    echo "date.timezone {$timezone}\n";

    foreach ($values as $value) {
        $batch = [new RawRowValues(['on' => $value, 'at' => $value, 'at_scl' => $value])];
        $expected = serialize(new DateTimeImmutable($value, new DateTimeZone('UTC')));
        $expectedScl = serialize(new DateTimeImmutable($value, new DateTimeZone('America/Santiago')));
        $phpRow = $php->hydrate($batch, $schema)->first();
        $nativeRow = $native->hydrate($batch, $schema)->first();

        printf(
            "%-14s %s %s date php:%s native:%s datetime php:%s native:%s scl %s php:%s native:%s\n",
            json_encode($value),
            $nativeRow->get('on')->format('Y-m-d H:i:s'),
            $nativeRow->get('on')->getTimezone()->getName(),
            serialize($phpRow->get('on')) === $expected ? 'yes' : 'NO',
            serialize($nativeRow->get('on')) === $expected ? 'yes' : 'NO',
            serialize($phpRow->get('at')) === $expected ? 'yes' : 'NO',
            serialize($nativeRow->get('at')) === $expected ? 'yes' : 'NO',
            $nativeRow->get('at_scl')->format('Y-m-d H:i:s'),
            serialize($phpRow->get('at_scl')) === $expectedScl ? 'yes' : 'NO',
            serialize($nativeRow->get('at_scl')) === $expectedScl ? 'yes' : 'NO',
        );
    }
}

foreach (['2026-02-30', '2026-13-01', '0000-01-01'] as $value) {
    foreach (['on' => date_schema('on'), 'at' => datetime_schema('at')] as $column => $definition) {
        $batch = [new RawRowValues([$column => $value])];

        foreach (['php' => $php, 'native' => $native] as $label => $hydrator) {
            try {
                $hydrator->hydrate($batch, schema($definition));
                echo "{$label} {$column} {$value}: FAIL no exception\n";
            } catch (Throwable $e) {
                echo "{$label} {$column} {$value}: ", $e::class, "\n";
            }
        }
    }
}

$halfPastMidnight = new DateTimeImmutable('2026-01-02 00:00:00.5', new DateTimeZone('UTC'));

foreach (['php' => $php, 'native' => $native] as $label => $hydrator) {
    echo $label, ' on 00:00:00.5: ', $hydrator->hydrate([new RawRowValues(['on' => $halfPastMidnight])], schema(date_schema('on')))->first()->get('on')->format('Y-m-d H:i:s.u e'), "\n";
}
?>
--EXPECT--
date.timezone UTC
"2026-01-02"   2026-01-02 00:00:00 UTC date php:yes native:yes datetime php:yes native:yes scl 2026-01-02 00:00:00 php:yes native:yes
"2024-02-29"   2024-02-29 00:00:00 UTC date php:yes native:yes datetime php:yes native:yes scl 2024-02-29 00:00:00 php:yes native:yes
"2026-01-02\n" 2026-01-02 00:00:00 UTC date php:yes native:yes datetime php:yes native:yes scl 2026-01-02 00:00:00 php:yes native:yes
"0001-01-01"   0001-01-01 00:00:00 UTC date php:yes native:yes datetime php:yes native:yes scl 0001-01-01 00:00:00 php:yes native:yes
"9999-12-31"   9999-12-31 00:00:00 UTC date php:yes native:yes datetime php:yes native:yes scl 9999-12-31 00:00:00 php:yes native:yes
"2026-09-06"   2026-09-06 00:00:00 UTC date php:yes native:yes datetime php:yes native:yes scl 2026-09-06 01:00:00 php:yes native:yes
date.timezone Europe/Warsaw
"2026-01-02"   2026-01-02 00:00:00 UTC date php:yes native:yes datetime php:yes native:yes scl 2026-01-02 00:00:00 php:yes native:yes
"2024-02-29"   2024-02-29 00:00:00 UTC date php:yes native:yes datetime php:yes native:yes scl 2024-02-29 00:00:00 php:yes native:yes
"2026-01-02\n" 2026-01-02 00:00:00 UTC date php:yes native:yes datetime php:yes native:yes scl 2026-01-02 00:00:00 php:yes native:yes
"0001-01-01"   0001-01-01 00:00:00 UTC date php:yes native:yes datetime php:yes native:yes scl 0001-01-01 00:00:00 php:yes native:yes
"9999-12-31"   9999-12-31 00:00:00 UTC date php:yes native:yes datetime php:yes native:yes scl 9999-12-31 00:00:00 php:yes native:yes
"2026-09-06"   2026-09-06 00:00:00 UTC date php:yes native:yes datetime php:yes native:yes scl 2026-09-06 01:00:00 php:yes native:yes
date.timezone America/Santiago
"2026-01-02"   2026-01-02 00:00:00 UTC date php:yes native:yes datetime php:yes native:yes scl 2026-01-02 00:00:00 php:yes native:yes
"2024-02-29"   2024-02-29 00:00:00 UTC date php:yes native:yes datetime php:yes native:yes scl 2024-02-29 00:00:00 php:yes native:yes
"2026-01-02\n" 2026-01-02 00:00:00 UTC date php:yes native:yes datetime php:yes native:yes scl 2026-01-02 00:00:00 php:yes native:yes
"0001-01-01"   0001-01-01 00:00:00 UTC date php:yes native:yes datetime php:yes native:yes scl 0001-01-01 00:00:00 php:yes native:yes
"9999-12-31"   9999-12-31 00:00:00 UTC date php:yes native:yes datetime php:yes native:yes scl 9999-12-31 00:00:00 php:yes native:yes
"2026-09-06"   2026-09-06 00:00:00 UTC date php:yes native:yes datetime php:yes native:yes scl 2026-09-06 01:00:00 php:yes native:yes
php on 2026-02-30: Flow\ETL\Exception\SchemaMismatchException
native on 2026-02-30: Flow\ETL\Exception\SchemaMismatchException
php at 2026-02-30: Flow\ETL\Exception\SchemaMismatchException
native at 2026-02-30: Flow\ETL\Exception\SchemaMismatchException
php on 2026-13-01: Flow\ETL\Exception\SchemaMismatchException
native on 2026-13-01: Flow\ETL\Exception\SchemaMismatchException
php at 2026-13-01: Flow\ETL\Exception\SchemaMismatchException
native at 2026-13-01: Flow\ETL\Exception\SchemaMismatchException
php on 0000-01-01: Flow\ETL\Exception\SchemaMismatchException
native on 0000-01-01: Flow\ETL\Exception\SchemaMismatchException
php at 0000-01-01: Flow\ETL\Exception\SchemaMismatchException
native at 0000-01-01: Flow\ETL\Exception\SchemaMismatchException
php on 00:00:00.5: 2026-01-02 00:00:00.000000 UTC
native on 00:00:00.5: 2026-01-02 00:00:00.000000 UTC
