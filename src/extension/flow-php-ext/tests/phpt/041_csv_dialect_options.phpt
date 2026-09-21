--TEST--
native CSV reading matches the PHP path for every dialect and read option
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

$raw = "\xEF\xBB\xBFid;name|x\tq\n1;'a;b'|\"c|d\"\t'e\tf'\n2;;|\t\n3;'x\\'y';\"p\\\"q\"|z\t\n";

foreach ([';', '|', "\t"] as $separator) {
    foreach (["'", '"'] as $enclosure) {
        foreach (['\\', ''] as $escape) {
            foreach ([[true, true, true], [false, true, true], [true, false, true], [true, true, false]] as [$withHeader, $emptyToNull, $removeBOM]) {
                assert_csv_identical(
                    sprintf('%s %s %s %d%d%d', json_encode($separator), $enclosure, json_encode($escape), $withHeader, $emptyToNull, $removeBOM),
                    csv_php_rows($raw, $separator, $enclosure, $escape, $withHeader, $emptyToNull, $removeBOM),
                    csv_native_rows($raw, $separator, $enclosure, $escape, $withHeader, $emptyToNull, $removeBOM),
                );
            }
        }
    }
}
?>
--EXPECT--
";" ' "\\" 111: identical
";" ' "\\" 011: identical
";" ' "\\" 101: identical
";" ' "\\" 110: identical
";" ' "" 111: identical
";" ' "" 011: identical
";" ' "" 101: identical
";" ' "" 110: identical
";" " "\\" 111: identical
";" " "\\" 011: identical
";" " "\\" 101: identical
";" " "\\" 110: identical
";" " "" 111: identical
";" " "" 011: identical
";" " "" 101: identical
";" " "" 110: identical
"|" ' "\\" 111: identical
"|" ' "\\" 011: identical
"|" ' "\\" 101: identical
"|" ' "\\" 110: identical
"|" ' "" 111: identical
"|" ' "" 011: identical
"|" ' "" 101: identical
"|" ' "" 110: identical
"|" " "\\" 111: identical
"|" " "\\" 011: identical
"|" " "\\" 101: identical
"|" " "\\" 110: identical
"|" " "" 111: identical
"|" " "" 011: identical
"|" " "" 101: identical
"|" " "" 110: identical
"\t" ' "\\" 111: identical
"\t" ' "\\" 011: identical
"\t" ' "\\" 101: identical
"\t" ' "\\" 110: identical
"\t" ' "" 111: identical
"\t" ' "" 011: identical
"\t" ' "" 101: identical
"\t" ' "" 110: identical
"\t" " "\\" 111: identical
"\t" " "\\" 011: identical
"\t" " "\\" 101: identical
"\t" " "\\" 110: identical
"\t" " "" 111: identical
"\t" " "" 011: identical
"\t" " "" 101: identical
"\t" " "" 110: identical
