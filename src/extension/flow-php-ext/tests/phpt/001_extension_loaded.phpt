--TEST--
flow_php extension is loaded and symbols are registered
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
var_dump(extension_loaded("flow_php"));
var_dump(class_exists('Flow\Floe\RustFloeEncoderNative'));
var_dump(class_exists('Flow\Floe\Exception\ExtensionException'));
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
