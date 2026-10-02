--TEST--
flow_php registers the flow-php/etl and adapter interfaces reflection-identical to the package files
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

$child = sprintf(
    'if (extension_loaded("flow_php")) { exit("flow_php is loaded in the child"); } require %s; echo interfaces_reflection();',
    var_export(__DIR__ . '/bootstrap.php', true),
);
$library = (string) shell_exec(escapeshellarg(PHP_BINARY) . ' -r ' . escapeshellarg($child));

echo str_starts_with($library, '{') ? 'library reflected' : $library, "\n";
echo $library === interfaces_reflection() ? 'identical' : "DIFF\nlibrary:  {$library}\nextension: " . interfaces_reflection(), "\n";
?>
--EXPECT--
library reflected
identical
