--TEST--
xml and xml_element columns decode through the ValueDecoder static fallback
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Floe\RowsDecoder;

use function Flow\ETL\DSL\{row, rows, int_entry, xml_entry, xml_element_entry};

$document = new DOMDocument();
$document->loadXML('<root attr="1"><item>a</item><item>b</item></root>');

$element = (new DOMDocument());
$element->loadXML('<item id="7">value</item>');

$rows = rows(
    row(
        int_entry('id', 1),
        xml_entry('doc', $document),
        xml_element_entry('el', $element->documentElement),
    ),
);

$frames = php_frames($rows);
$actual = decoder_decode_frames(new RowsDecoder(), $frames);

assert_rows_identical(php_decode_frames($frames), $actual);

var_dump(get_class($actual[0]->get('doc')->value()));
var_dump(trim($actual[0]->get('doc')->value()->saveXML()));
var_dump(get_class($actual[0]->get('el')->value()));
var_dump($actual[0]->get('el')->value()->getAttribute('id'));
?>
--EXPECT--
identical
string(11) "DOMDocument"
string(72) "<?xml version="1.0"?>
<root attr="1"><item>a</item><item>b</item></root>"
string(10) "DOMElement"
string(1) "7"
