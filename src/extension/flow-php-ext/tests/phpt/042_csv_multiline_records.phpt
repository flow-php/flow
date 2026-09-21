--TEST--
native CSV records span lines exactly like CSVLineReader: quoted LF and CRLF, open at EOF, blank lines, BOM before a quote
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

$cases = [
    'quoted LF' => "a,b\n\"x\ny\",1\n2,3\n",
    'quoted CRLF' => "a,b\r\n\"x\r\ny\",1\r\n2,3\r\n",
    'escaped quote then LF' => "a,b\n\"x\\\"\ny\",1\n",
    'doubled quote then LF' => "a,b\n\"x\"\"\ny\",1\n",
    'bare quote in unenclosed field' => "a,b\nx\"y,1\n\"p\",2\n",
    'open at EOF' => "a,b\n1,\"never closed\nstill open\n",
    'open at EOF without LF' => "a,b\n1,\"never closed",
    'blank lines' => "a,b\n\n1,2\n\n\n",
    'extra CRs' => "a,b\r\r\n1,2\r\r\r\n",
    'BOM then quote' => "\xEF\xBB\xBF\"a\",b\n1,2\n",
    'BOM then open quote' => "\xEF\xBB\xBF\"a\nb\",c\n1,2\n",
];

foreach ($cases as $case => $raw) {
    assert_csv_identical($case, csv_php_rows($raw, ',', '"', '\\'), csv_native_rows($raw, ',', '"', '\\'));
}
?>
--EXPECT--
quoted LF: identical
quoted CRLF: identical
escaped quote then LF: identical
doubled quote then LF: identical
bare quote in unenclosed field: identical
open at EOF: identical
open at EOF without LF: identical
blank lines: identical
extra CRs: identical
BOM then quote: identical
BOM then open quote: identical
