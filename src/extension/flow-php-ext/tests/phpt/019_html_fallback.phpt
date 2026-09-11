--TEST--
html and html_element columns decode through the ValueDecoder static fallback (PHP 8.4+)
--SKIPIF--
<?php
if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded");
if (!class_exists('Dom\HTMLDocument')) die("skip Dom\HTMLDocument requires PHP 8.4+");
?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\html_element_schema;
use function Flow\ETL\DSL\html_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_html_element;

$schema = schema(int_schema('id'), html_schema('doc'), html_element_schema('el'));

$frames = php_frames(rows($schema, row([
    'id' => 1,
    'doc' => type_html()->cast('<!DOCTYPE html><html><head></head><body><p id="x">hello</p></body></html>'),
    'el' => type_html_element()->cast('<div class="c">content</div>'),
])));
$actual = ext_decode_frames($frames);

var_dump(php_frames(rows($schema, ...$actual)) === php_frames(rows($schema, ...php_decode_frames($frames))));

var_dump(get_class($actual[0]->get('doc')));
var_dump($actual[0]->get('doc')->getElementById('x')->textContent);
var_dump($actual[0]->get('el')->getAttribute('class'));
?>
--EXPECT--
bool(true)
string(16) "Dom\HTMLDocument"
string(5) "hello"
string(1) "c"
