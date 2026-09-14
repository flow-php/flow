--TEST--
extension version is the release tag, or the last tag with commit distance and hash
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
var_dump(preg_match('/^\d+\.\d+\.\d+(\+\d+\.g[0-9a-f]+)?$/', phpversion('flow_php')));
?>
--EXPECT--
int(1)
