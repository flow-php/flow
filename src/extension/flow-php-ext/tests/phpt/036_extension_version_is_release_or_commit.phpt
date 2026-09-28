--TEST--
extension version is the release tag, or the next minor's dev pre-release with commit distance and hash
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
var_dump(preg_match('/^\d+\.\d+\.\d+(-dev(\+\d+\.g[0-9a-f]+)?)?$/', phpversion('flow_php')));
?>
--EXPECT--
int(1)
