--TEST--
arrow registers the flow-php/parquet interfaces reflection-identical to the package files
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

$child = sprintf(
    'if (extension_loaded("arrow")) { exit("arrow is loaded in the child"); } require %s; echo arrow_interfaces_reflection();',
    var_export(__DIR__ . '/bootstrap.php', true),
);
$library = (string) shell_exec(escapeshellarg(PHP_BINARY) . ' -r ' . escapeshellarg($child));

echo str_starts_with($library, '{') ? 'library reflected' : $library, "\n";
echo $library === arrow_interfaces_reflection() ? 'identical' : "DIFF\nlibrary:  {$library}\nextension: " . arrow_interfaces_reflection(), "\n";

foreach ([['Flow\Parquet\ParquetEngine', 'writeRows', 4], ['Flow\Parquet\ParquetFileWriter', 'writeBatch', 0]] as [$class, $method, $position]) {
    echo $class, '::', $method, ': ', (new ReflectionMethod($class, $method))->getParameters()[$position]->getType(), "\n";
}
?>
--EXPECT--
library reflected
identical
Flow\Parquet\ParquetEngine::writeRows: iterable
Flow\Parquet\ParquetFileWriter::writeBatch: iterable
