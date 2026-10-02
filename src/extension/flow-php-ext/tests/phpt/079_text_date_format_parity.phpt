--TEST--
The native writers format a datetime and a date as DateTimeInterface::format() does: every rendered letter and random formats, five zones, instants around gaps, overlaps and extreme years
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\CSVWriteOptions;
use Flow\ETL\Adapter\CSV\RustCSVEncoder;
use Flow\ETL\Adapter\CSV\PhpCSVEncoder;

use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\schema;

mt_srand(79);

$letters = str_split('YymndjHGisuveTPpOZUc');
$formats = array_map(static fn(string $letter): string => $letter . ' | ' . $letter, $letters);
$formats[] = implode('|', $letters);
$formats[] = 'Y-m-d\TH:i:s.uP';
$formats[] = '\Y\m\d Y \\\\ \u u';

for ($i = 0; $i < 60; $i++) {
    $format = '';

    for ($j = mt_rand(1, 8); $j > 0; $j--) {
        $format .= [$letters[mt_rand(0, count($letters) - 1)], ['-', ':', ' ', '.', '/', '\\T', '\\e', 'ż', ','][mt_rand(0, 8)]][mt_rand(0, 1)];
    }

    $formats[] = $format;
}

$at = static fn(int $second, int $micro = 0): DateTimeImmutable => (new DateTimeImmutable('@' . $second))->modify('+' . $micro . ' usec');
// each side of the 2026 gaps and overlaps of Europe/Warsaw, America/New_York and Australia/Lord_Howe (a 30 minute shift)
$edges = [1_774_745_999, 1_774_746_000, 1_792_889_999, 1_792_890_000, 1_772_953_199, 1_772_953_200, 1_793_512_799, 1_793_512_800, 1_775_316_599, 1_775_316_600, 1_791_041_399, 1_791_041_400];
$instants = [$at(0), $at(-1, 999_999), $at(0, 999_999), $at(-2_208_988_800), $at(-2_524_521_600, 1), $at(-5_000_000_000, 500_000)];

foreach ($edges as $edge) {
    $instants[] = $at($edge, mt_rand(0, 1) * 999_999);
}

for ($i = 0; $i < 200; $i++) {
    $instants[] = $at(mt_rand(-3_000_000_000, 5_000_000_000), mt_rand(0, 999_999));
}

// years 999, 0, -1, 10 000 and 100 000: one row each, a named zone lists its transitions over the batch's range
$extremes = [-30_641_760_000, -62_167_219_200, -62_198_755_200, 253_402_300_800, 3_093_527_980_800];
$different = 0;
$compared = 0;

foreach (['UTC', '+02:30', 'Europe/Warsaw', 'America/New_York', 'Australia/Lord_Howe'] as $zone) {
    $schema = schema(datetime_schema('at', zone: $zone));
    $batches = [array_map(static fn(DateTimeImmutable $instant): array => ['at' => $instant], $instants)];

    foreach ($extremes as $second) {
        $batches[] = [['at' => $at($second)], ['at' => $at($second + 86_399, 999_999)]];
    }

    foreach ($batches as $values) {
        $rows = native_rows($schema, $values);

        foreach ($formats as $format) {
            $recorder = new RecordingPhpCSVEncoder(new PhpCSVEncoder(new CSVWriteOptions("\x01", "\x02", '', "\n", $format, 'Y-m-d')));
            $writer = new RustCSVEncoder("\x01", "\x02", '', "\n", $format, 'Y-m-d', $recorder);
            $expected = '';

            foreach ($values as $value) {
                $expected .= $value['at']->setTimezone(new DateTimeZone($zone))->format($format) . "\n";
            }

            $compared++;
            // a field holding a space is enclosed: the enclosure is dropped, no format writes that byte
            $written = str_replace("\x02", '', $writer->encode($rows));

            if ($recorder->columns !== []) {
                throw new LogicException("the format {$format} is not rendered natively");
            }

            if ($written !== $expected && ++$different <= 5) {
                echo "{$zone} {$format}\n  php:    ", substr(json_encode($expected), 0, 300), "\n  native: ", substr(json_encode($written), 0, 300), "\n";
            }
        }
    }
}

echo "datetime: {$different} of {$compared} differ\n";

$schema = schema(date_schema('on'));
$days = [0, -1, 1, 20_455, -25_567, -719_528, -719_893, 2_932_896, 35_804_721];
$different = 0;

foreach ($formats as $format) {
    $writer = new RustCSVEncoder("\x01", "\x02", '', "\n", DATE_ATOM, $format, new PhpCSVEncoder(new CSVWriteOptions("\x01", "\x02", '', "\n", DATE_ATOM, $format)));
    $expected = '';

    foreach ($days as $day) {
        $expected .= (new DateTimeImmutable('@' . ($day * 86_400)))->setTimezone(new DateTimeZone('UTC'))->format($format) . "\n";
    }

    $written = str_replace("\x02", '', $writer->encode(native_rows($schema, array_map(static fn(int $day): array => ['on' => new DateTimeImmutable('@' . ($day * 86_400))], $days))));

    if ($written !== $expected && ++$different <= 5) {
        echo "date {$format}\n  php:    ", substr(json_encode($expected), 0, 300), "\n  native: ", substr(json_encode($written), 0, 300), "\n";
    }
}

echo "date: {$different} of " . count($formats) . " differ\n";

foreach (['D, d M Y', 'Y-m-d I', 'N', 'Y\\'] as $format) {
    $recorder = new RecordingPhpCSVEncoder(new PhpCSVEncoder(new CSVWriteOptions(',', '"', '\\', "\n", $format, $format)));
    $rows = native_rows(schema(datetime_schema('at'), date_schema('on')), [['at' => new DateTimeImmutable('2024-01-02 03:04:05'), 'on' => new DateTimeImmutable('2024-01-02')]]);
    (new RustCSVEncoder(',', '"', '\\', "\n", $format, $format, $recorder))->encode($rows);
    printf("%s: %s\n", $format, json_encode(recorded_names($recorder->columns, $rows)));
}
?>
--EXPECT--
datetime: 0 of 2490 differ
date: 0 of 83 differ
D, d M Y: ["at","on"]
Y-m-d I: ["at","on"]
N: ["at","on"]
Y\: ["at","on"]
