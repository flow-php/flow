--TEST--
extension version is the release tag, or the next minor's dev pre-release with commit distance and hash
--FILE--
<?php
var_dump(preg_match('/^\d+\.\d+\.\d+(-dev(\+\d+\.g[0-9a-f]+)?)?$/', phpversion('arrow')));
?>
--EXPECT--
int(1)
