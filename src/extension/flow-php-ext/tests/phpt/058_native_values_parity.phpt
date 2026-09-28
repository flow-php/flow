--TEST--
native columns read every row as the PHP columns read it: at(), value(), Rows::values(), physicals(), values()
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Column\DefaultBackend;
use Flow\ETL\Column\PhpBackend;
use Flow\Floe\FrameDecoder;

use function Flow\ETL\DSL\{datetime_schema, schema, time_schema};

$schema = all_types_schema();
$php = php_rows($schema, all_types_values());
$native = native_rows($schema, all_types_values());
$differences = [];

foreach (array_keys($schema->definitions()) as $name) {
    $expected = $php->column((string) $name);
    $actual = $native->column((string) $name);

    foreach (['physicals', 'values'] as $read) {
        if (comparable($expected->{$read}()) !== comparable($actual->{$read}())) {
            $differences[] = "{$name} {$read}()";
        }
    }

    for ($i = 0; $i < $php->count(); $i++) {
        foreach (['at', 'value', 'isNull'] as $read) {
            if (comparable($expected->{$read}($i)) !== comparable($actual->{$read}($i))) {
                $differences[] = "{$name} {$read}({$i})";
            }
        }
    }
}

for ($i = 0; $i < $php->count(); $i++) {
    if (comparable($php->values($i)) !== comparable($native->values($i))) {
        $differences[] = "Rows::values({$i})";
    }
}

echo $differences === [] ? 'all types identical' : implode("\n", $differences), "\n";

$observe = static fn(DateTimeInterface $value): array => [
    serialize($value),
    $value->format('Y-m-d H:i:s.u T e P U Z'),
    $value->modify('+1 month')->format('Y-m-d H:i:s.u T e'),
    $value->modify('midnight')->format('Y-m-d H:i:s.u T e U'),
    $value->setTime(1, 2, 3, 4)->format('Y-m-d H:i:s.u T e U'),
    $value->add(new DateInterval('P1DT25H'))->format('Y-m-d H:i:s.u T e U'),
];
$instants = [];

foreach (['UTC', 'Z', 'Europe/Warsaw', 'America/New_York', 'Australia/Lord_Howe', '+00:00', '+02:00', '-05:30', 'CEST', 'EST'] as $timezone) {
    foreach ([
        '1970-01-01 00:00:00.000000 UTC',
        '1969-12-31 23:59:59.999999 UTC',
        '1969-07-20 20:17:00.5 UTC',
        '2026-03-29 00:59:59.999999 UTC',
        '2026-03-29 01:00:00.000001 UTC',
        '2026-10-25 00:30:00 UTC',
        '2026-10-25 01:30:00 UTC',
        '2038-01-19 03:14:08.000001 UTC',
        '0001-01-01 00:00:00 UTC',
        '9999-12-31 23:59:59.999999 UTC',
        '-0044-03-15 12:00:00 UTC',
    ] as $instant) {
        $instants[] = ['at' => (new DateTimeImmutable($instant))->setTimezone(new DateTimeZone($timezone))];
    }
}

$frame = php_rows(schema(datetime_schema('at')), $instants)->encodeFrame();

foreach (['UTC', 'Europe/Warsaw', '+05:30'] as $zone) {
    $zoned = schema(datetime_schema('at', zone: $zone));
    $expected = array_map($observe, (new FrameDecoder())->decode($frame, $zoned, new PhpBackend())->column('at')->values());
    $actual = array_map($observe, (new FrameDecoder())->decode($frame, $zoned, new DefaultBackend())->column('at')->values());

    echo "zone {$zone}: ", serialize($expected) === serialize($actual) ? 'identical' : 'DIFF', "\n";
}

foreach (native_rows(schema(time_schema('t')), [['t' => new DateInterval('PT1H')]])->column('t')->values() as $interval) {
    var_dump($interval->days);
}
?>
--EXPECT--
all types identical
zone UTC: identical
zone Europe/Warsaw: identical
zone +05:30: identical
bool(false)
