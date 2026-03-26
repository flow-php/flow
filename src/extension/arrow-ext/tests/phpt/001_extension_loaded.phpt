--TEST--
arrow extension is loaded and classes are registered
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
var_dump(extension_loaded("arrow"));
var_dump(class_exists('Flow\Arrow\Parquet\Reader'));
var_dump(class_exists('Flow\Arrow\Parquet\Writer'));
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
