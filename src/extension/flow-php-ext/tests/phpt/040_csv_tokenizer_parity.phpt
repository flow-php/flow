--TEST--
native CSV fields match str_getcsv() byte for byte on every tokenizer edge case
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\RustCSVReaderNative;

$cases = [
    'plain' => '612c622c63',
    'quoted' => '2261222c226222',
    'backslash_quote' => '22615c2262222c63',
    'doubled_quote' => '2261222262222c63',
    'space_before_enclosure' => '2261222c2020226222',
    'junk_after_close' => '612c226222782c63',
    'unterminated' => '22756e7465726d696e617465642c61',
    'blank' => '',
    'trailing_sep' => '612c622c',
];

foreach ($cases as $case => $hex) {
    $input = hex2bin($hex);
    $reader = new RustCSVReaderNative(',', '"', '\\', false, false, false);
    $reader->feed($input . "\n");
    $reader->finish();

    $fields = array_values($reader->next(10)[0]->values);
    $expected = str_getcsv($input, ',', '"', '\\');

    echo $case, ': ', $fields === $expected ? 'identical' : 'FAIL ' . json_encode(array_map(static fn(?string $f): ?string => $f === null ? null : bin2hex($f), $fields)), "\n";
}
?>
--EXPECT--
plain: identical
quoted: identical
backslash_quote: identical
doubled_quote: identical
space_before_enclosure: identical
junk_after_close: identical
unterminated: identical
blank: identical
trailing_sep: identical
