--TEST--
native datetime decode builds the same DateTimeImmutable as createFromFormat('U.u')->setTimezone(), state and behaviour
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

use Flow\ETL\Row\PhpRowHydrator;
use Flow\Floe\PhpFloeEncoder;
use Flow\Floe\RustFloeEncoderNative;

function observe(DateTimeInterface $value): array
{
    $fixed = new DateTimeImmutable('2000-02-29 12:34:56.654321', new DateTimeZone('Asia/Kolkata'));

    return [
        serialize($value),
        var_export($value, true),
        $value->format('Y-m-d H:i:s.u T e P p O U I Z L N z t'),
        $value->getTimestamp(),
        $value->getOffset(),
        $value->getTimezone()->getName(),
        $value->modify('+1 month')->format('Y-m-d H:i:s.u T e'),
        $value->modify('midnight')->format('Y-m-d H:i:s.u T e U'),
        $value->modify('last day of next month noon')->format('Y-m-d H:i:s.u T e'),
        $value->setTime(1, 2, 3, 4)->format('Y-m-d H:i:s.u T e U'),
        $value->setDate(2024, 2, 29)->format('Y-m-d H:i:s.u T e U'),
        $value->add(new DateInterval('P1DT25H'))->format('Y-m-d H:i:s.u T e U'),
        $value->sub(new DateInterval('P1M'))->format('Y-m-d H:i:s.u T e U'),
        $value->diff($fixed)->format('%R %y %m %d %h %i %s %f %a'),
        $value->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d H:i:s.u T e'),
        $value->setTimestamp(86400)->format('Y-m-d H:i:s.u T e'),
        $value == $fixed,
        $value < $fixed,
    ];
}

$timezones = ['UTC', 'Z', 'Europe/Warsaw', 'America/New_York', 'Australia/Lord_Howe', '+00:00', '+02:00', '-05:30', 'CEST', 'EST'];
$instants = [
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
];

$schema = schema(datetime_schema('at'));
$schemaBody = json_encode($schema->normalize(), JSON_THROW_ON_ERROR);
$values = [];

foreach ($timezones as $timezone) {
    foreach ($instants as $instant) {
        $values[] = (new DateTimeImmutable($instant))->setTimezone(new DateTimeZone($timezone));
    }
}

$bodies = (new PhpFloeEncoder($schema))->encode((new PhpRowHydrator())->dehydrate(rows($schema, ...array_map(static fn($at) => row(['at' => $at]), $values))));
$php = (new PhpFloeEncoder($schema))->decode($bodies);
$native = (new RustFloeEncoderNative())->decode($bodies, $schemaBody);

$differences = 0;

foreach ($php as $index => $expected) {
    if (observe($expected->values['at']) !== observe($native[$index]->values['at'])) {
        $differences++;
        echo 'FAIL: ', $values[$index]->format('Y-m-d H:i:s.u e'), "\n";
        var_dump(array_diff_assoc(observe($expected->values['at']), observe($native[$index]->values['at'])));
    }
}

echo count($php), ' values, ', $differences, " differences\n";

$overlong = pack('P', 0) . pack('V', 1_000_000) . pack('V', 3) . 'UTC';

try {
    (new RustFloeEncoderNative())->decode(["\x01" . $overlong], $schemaBody);
    echo "FAIL: no exception\n";
} catch (Flow\Floe\Exception\ExtensionException $e) {
    echo $e->getMessage(), "\n";
}

try {
    (new PhpFloeEncoder($schema))->decode(["\x01" . $overlong]);
    echo "FAIL: no exception\n";
} catch (Flow\Floe\Exception\FloeException $e) {
    echo $e->getMessage(), "\n";
}
?>
--EXPECT--
110 values, 0 differences
flow_php failed to restore datetime from timestamp "0"
Floe failed to restore datetime from timestamp "0"
