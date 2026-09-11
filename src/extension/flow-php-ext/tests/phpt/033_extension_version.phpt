--TEST--
a build hydrating schema-carrying Rows reports at least version 0.3.0
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
var_dump(version_compare(phpversion('flow_php'), '0.3.0', '>='));
?>
--EXPECT--
bool(true)
