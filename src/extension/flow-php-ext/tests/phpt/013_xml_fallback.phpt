--TEST--
xml and xml_element columns decode through the ValueDecoder static fallback
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\ETL\DSL\xml_schema;

$document = new DOMDocument();
$document->loadXML('<root attr="1"><item>a</item><item>b</item></root>');

$element = (new DOMDocument());
$element->loadXML('<item id="7">value</item>');

$schema = schema(int_schema('id'), xml_schema('doc'), xml_element_schema('el'));

$frames = php_frames(rows($schema, row(['id' => 1, 'doc' => $document, 'el' => $element->documentElement])));
$actual = ext_decode_frames($frames);

// DOM values are not serializable on their own, so the decoded rows are compared
// through their re-encoded frame bodies instead of through serialize().
var_dump(php_frames(rows($schema, ...$actual)) === php_frames(rows($schema, ...php_decode_frames($frames))));

var_dump(get_class($actual[0]->get('doc')));
var_dump(trim($actual[0]->get('doc')->saveXML()));
var_dump(get_class($actual[0]->get('el')));
var_dump($actual[0]->get('el')->getAttribute('id'));
?>
--EXPECT--
bool(true)
string(11) "DOMDocument"
string(72) "<?xml version="1.0"?>
<root attr="1"><item>a</item><item>b</item></root>"
string(10) "DOMElement"
string(1) "7"
