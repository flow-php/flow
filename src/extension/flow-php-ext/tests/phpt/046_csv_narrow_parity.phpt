--TEST--
native narrowing matches StringTypeNarrower::narrow() on its fixtures, the byte-level traps and a fuzz corpus
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\RustColumnFoldNative;
use Flow\ETL\Schema\Inference\InferredTypes;

use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_string;

$fixtures = string_type_narrower_fixture_values();
$corpus = csv_narrow_corpus();

var_dump(count($fixtures) >= 80);
var_dump(count($corpus) >= 300);
assert_narrow_parity('default candidates', InferredTypes::default()->toArray(), $corpus);

// without integer, '20240305' reaches the temporal rung
$temporal = [
    '2026-01-02T03:04',
    '2026-01-02 03:04',
    '2026-01-02T03:04:05',
    '2026-01-02T03:04:05.1',
    '2026-01-02T03:04:05.123456789',
    '2026-01-02T03:04:05.1234567890',
    '2026-01-02T03:04:05Z',
    '2026-01-02T03:04:05+02',
    '2026-01-02T03:04+99',
    '2026-01-02T03:04:05-0230',
    '2026-01-02T03:04:05.5+02:00',
    '2026-01-02T03:04+24:59',
    '2024-02-29T00:00',
    '2026-01-02T24:00',
    '2026-01-02T03:04:60',
    '2026-01-02T25:00',
    '2026-01-02T03:60',
    '2026-01-02T03:04:61',
    '2026-01-02T03:04+25:00',
    '2026-01-02T03:04+0060',
    '2026-02-30T03:04',
    '2026-13-01T03:04',
    '2026-01-02',
    '2024-02-29',
    '2026-02-30',
    '2026-13-01',
    '0000-01-01',
    '2024-01',
    'March 5, 2024',
    '05/03/2024',
    '02-Jun-2022',
    '20240305',
    '12345678',
    '2024-03-05 noon',
    'tomorrow 2024-03-05',
    'now',
];
$candidates = [type_date(), type_datetime(), type_string()];
$native = new RustColumnFoldNative([], ['date', 'datetime', 'string']);

foreach ($temporal as $value) {
    printf("%-32s %s\n", $value, $native->narrowOne($value));
}

assert_narrow_parity('temporal candidates', $candidates, $temporal);
?>
--EXPECT--
bool(true)
bool(true)
default candidates: identical
2026-01-02T03:04                 datetime
2026-01-02 03:04                 datetime
2026-01-02T03:04:05              datetime
2026-01-02T03:04:05.1            datetime
2026-01-02T03:04:05.123456789    datetime
2026-01-02T03:04:05.1234567890   datetime
2026-01-02T03:04:05Z             datetime
2026-01-02T03:04:05+02           datetime
2026-01-02T03:04+99              datetime
2026-01-02T03:04:05-0230         datetime
2026-01-02T03:04:05.5+02:00      datetime
2026-01-02T03:04+24:59           datetime
2024-02-29T00:00                 datetime
2026-01-02T24:00                 datetime
2026-01-02T03:04:60              datetime
2026-01-02T25:00                 string
2026-01-02T03:60                 string
2026-01-02T03:04:61              string
2026-01-02T03:04+25:00           string
2026-01-02T03:04+0060            string
2026-02-30T03:04                 string
2026-13-01T03:04                 string
2026-01-02                       date
2024-02-29                       date
2026-02-30                       string
2026-13-01                       string
0000-01-01                       string
2024-01                          string
March 5, 2024                    date
05/03/2024                       date
02-Jun-2022                      date
20240305                         date
12345678                         string
2024-03-05 noon                  datetime
tomorrow 2024-03-05              datetime
now                              string
temporal candidates: identical
