--TEST--
ISO date-times with a Z or ±HH:MM suffix become the micros DateTimeType::cast() yields, through append() and nextColumns()
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\RustCSVOpenSource;
use Flow\ETL\Column\RustBackend;
use Flow\ETL\Column\PhpBackend;
use Flow\Filesystem\Stream\MemorySourceStream;

use function Flow\ETL\DSL\{datetime_schema, schema};

mt_srand(66);
$two = static fn(int $value): string => str_pad((string) $value, 2, '0', STR_PAD_LEFT);
$leap = static fn(int $year): bool => ($year % 4 === 0 && $year % 100 !== 0) || $year % 400 === 0;
$year = static fn(): int => match (mt_rand(0, 3)) {
    0 => mt_rand(1, 1969),
    1 => mt_rand(1970, 2038),
    2 => mt_rand(2039, 9999),
    default => [1, 4, 1900, 1968, 1969, 1970, 2000, 2024, 2038, 2100, 2400, 9996, 9999][mt_rand(0, 12)],
};
$fraction = static fn(): string => ($digits = mt_rand(0, 6)) === 0
    ? ''
    : '.' . implode('', array_map(static fn(): int => mt_rand(0, 9), range(1, $digits)));
$suffix = static function () use ($two): string {
    if (mt_rand(0, 3) === 0) {
        return 'Z';
    }

    $minutes = mt_rand(-12 * 60, 14 * 60);
    $minutes -= mt_rand(0, 1) === 0 ? $minutes % 15 : 0;

    return ($minutes < 0 ? '-' : '+') . $two(intdiv(abs($minutes), 60)) . ':' . $two(abs($minutes) % 60);
};

$strings = [];

for ($i = 0; $i < 3000; $i++) {
    $y = $year();
    $m = mt_rand(1, 12);
    $days = match ($m) {
        2 => $leap($y) ? 29 : 28,
        4, 6, 9, 11 => 30,
        default => 31,
    };
    $d = match (mt_rand(0, 3)) {
        0 => $days,
        1 => 1,
        default => mt_rand(1, $days),
    };

    if (mt_rand(0, 9) === 0) {
        [$y, $m, $d] = [[4, 2000, 2024, 2400, 9996][mt_rand(0, 4)], 2, 29];
    }

    $strings[] = sprintf(
        '%s-%s-%sT%s:%s:%s%s%s',
        str_pad((string) $y, 4, '0', STR_PAD_LEFT),
        $two($m),
        $two($d),
        $two(mt_rand(0, 23)),
        $two(mt_rand(0, 59)),
        $two(mt_rand(0, 59)),
        $fraction(),
        $suffix(),
    );
}

$nearMisses = [
    '2026-01-02T03:04:05', '2026-01-02 03:04:05Z', '2026-01-02t03:04:05Z', '2026-01-02T03:04:05z', '2026-01-02T03:04Z',
    '2026-01-02T03:04:05.1234567Z', '2026-01-02T03:04:05.123456789+01:00', '2026-01-02T03:04:05,5Z', '2026-01-02T03:04:05.Z',
    '2026-01-02T03:04:05+01', '2026-01-02T03:04:05+0100', '2026-01-02T03:04:05+24:00', '2026-01-02T03:04:05+01:60',
    '2026-01-02T24:00:00Z', '2026-12-31T23:59:60Z', '0000-01-01T00:00:00Z', '+2026-01-02T03:04:05Z', '-2026-01-02T03:04:05Z',
    '12026-01-02T03:04:05Z', '2023-02-29T00:00:00Z', '2026-04-31T00:00:00Z', '2026-13-01T00:00:00Z', "2026-01-02T03:04:05Z\n",
    ' 2026-01-02T03:04:05Z', '2026-01-02T03:04:05Z ', '2026-01-02T03:04:05ZZ', '2026-01-02T03:04:05+01:00Z',
];

$physicals = static function (object $backend, object $definition, array $values): array {
    $builder = $backend->builder($definition);
    $builder->appendMany($values);

    return $builder->finish()->physicals();
};
$csv = static function (object $definition, array $values): array {
    $source = new RustCSVOpenSource(new MemorySourceStream("v\n" . implode("\n", $values) . "\n"), ',', '"', '', true, true, true);

    return iterator_to_array($source->batches(schema($definition), count($values) + 1, new RustBackend()), false)[0]->column('v')->physicals();
};

foreach (['UTC', 'Europe/Warsaw', 'Pacific/Kiritimati'] as $zone) {
    $definition = datetime_schema('v', zone: $zone);
    $expected = $physicals(new PhpBackend(), $definition, $strings);
    $appended = $physicals(new RustBackend(), $definition, $strings);
    $read = $csv($definition, $strings);
    $differ = static fn(array $actual): array => array_keys(array_filter(
        $expected,
        static fn(int $micros, int $i): bool => $actual[$i] !== $micros,
        ARRAY_FILTER_USE_BOTH,
    ));

    foreach (['append()' => $appended, 'nextColumns()' => $read] as $lane => $actual) {
        $wrong = $differ($actual);
        echo "{$zone} {$lane}: ", $wrong === []
            ? count($strings) . ' strings, identical micros'
            : implode("\n", array_map(static fn(int $i): string => "{$strings[$i]} php {$expected[$i]} native {$actual[$i]}", array_slice($wrong, 0, 10))), "\n";
    }

    $identical = 0;

    foreach ($nearMisses as $value) {
        $php = outcome(static fn(): array => $physicals(new PhpBackend(), $definition, [$value]));
        $native = outcome(static fn(): array => $physicals(new RustBackend(), $definition, [$value]));

        $php === $native ? $identical++ : print(json_encode($value) . "\n  php:    {$php}\n  native: {$native}\n");
    }

    echo "{$zone} near misses: {$identical} of " . count($nearMisses) . " identical\n";
}
?>
--EXPECT--
UTC append(): 3000 strings, identical micros
UTC nextColumns(): 3000 strings, identical micros
UTC near misses: 27 of 27 identical
Europe/Warsaw append(): 3000 strings, identical micros
Europe/Warsaw nextColumns(): 3000 strings, identical micros
Europe/Warsaw near misses: 27 of 27 identical
Pacific/Kiritimati append(): 3000 strings, identical micros
Pacific/Kiritimati nextColumns(): 3000 strings, identical micros
Pacific/Kiritimati near misses: 27 of 27 identical
