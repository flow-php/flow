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


use function Flow\ETL\DSL\{row, rows, int_entry, html_entry, html_element_entry};

$rows = rows(
    row(
        int_entry('id', 1),
        html_entry('doc', '<!DOCTYPE html><html><body><p id="x">hello</p></body></html>'),
        html_element_entry('el', '<div class="c">content</div>'),
    ),
);

$frames = php_frames($rows);
$actual = ext_decode_frames($frames);

$expected = php_decode_frames($frames);
var_dump(php_frames(rows(...$actual)) === php_frames(rows(...$expected)));

var_dump(get_class($actual[0]->get('doc')->value()));
var_dump($actual[0]->get('doc')->value()->getElementById('x')->textContent);
var_dump($actual[0]->get('el')->value()->getAttribute('class'));
?>
--EXPECT--
bool(true)
string(16) "Dom\HTMLDocument"
string(5) "hello"
string(1) "c"
