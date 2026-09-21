--TEST--
native CSV rows match the PHP path over a seeded fuzz corpus, every dialect and chunk size
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

$named = [
    'blanks before a multi-line enclosure' => [',', "a, \"b\nc\",d\ne\n"],
    'whitespace separator before an enclosure' => ["\t", "h1\th2\th3\na\t\t\"b\"\n"],
    'trailing carriage return in an unenclosed field' => [',', "h1,h2\na\r,b\n"],
];

foreach ($named as $case => [$separator, $raw]) {
    assert_csv_identical(
        $case,
        csv_php_rows($raw, $separator, '"', '\\', false),
        csv_native_rows($raw, $separator, '"', '\\', false),
    );
}

mt_srand(50);
$alphabet = ['a', 'b', ' ', "\t", ',', ';', '"', "'", '\\', "\n", "\r"];
$dialects = [[',', '"', '\\'], [';', '"', '\\'], [',', "'", '\\'], [',', '"', ''], ["\t", '"', '\\']];
$mismatches = 0;

for ($i = 0; $i < 2000; $i++) {
    $raw = '';

    for ($j = mt_rand(0, 40); $j > 0; $j--) {
        $raw .= $alphabet[mt_rand(0, count($alphabet) - 1)];
    }

    [$separator, $enclosure, $escape] = $dialects[mt_rand(0, count($dialects) - 1)];
    $withHeader = mt_rand(0, 1) === 1;
    $emptyToNull = mt_rand(0, 1) === 1;
    $expected = csv_php_rows($raw, $separator, $enclosure, $escape, $withHeader, $emptyToNull);

    foreach ([1, 7, 4096] as $chunk) {
        if (csv_native_rows($raw, $separator, $enclosure, $escape, $withHeader, $emptyToNull, true, $chunk) !== $expected) {
            $mismatches++;
            echo 'MISMATCH ', json_encode([$raw, $separator, $enclosure, $escape, $withHeader, $emptyToNull, $chunk]), "\n";
        }
    }
}

echo "fuzz mismatches: {$mismatches}\n";
?>
--EXPECT--
blanks before a multi-line enclosure: identical
whitespace separator before an enclosure: identical
trailing carriage return in an unenclosed field: identical
fuzz mismatches: 0
