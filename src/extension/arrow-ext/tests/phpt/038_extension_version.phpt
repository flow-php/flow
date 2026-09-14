--TEST--
extension version is the release tag, or the last tag with commit distance and hash
--FILE--
<?php
var_dump(preg_match('/^\d+\.\d+\.\d+(\+\d+\.g[0-9a-f]+)?$/', phpversion('arrow')));
?>
--EXPECT--
int(1)
