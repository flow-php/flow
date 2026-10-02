--TEST--
flow_php extension is loaded and registers the package interfaces and the Rust classes implementing them
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
var_dump(extension_loaded("flow_php"));

$classes = (new ReflectionExtension('flow_php'))->getClassNames();
sort($classes);

foreach ($classes as $class) {
    $reflection = new ReflectionClass($class);
    echo $reflection->isInterface() ? 'interface ' : 'class ', $class, $reflection->isInterface() ? '' : ' implements ' . implode(', ', array_diff($reflection->getInterfaceNames(), ['Traversable'])), "\n";
}

var_dump(class_exists('Flow\Floe\Exception\ExtensionException', false) === false);
?>
--EXPECT--
bool(true)
interface Flow\ETL\Adapter\CSV\CSVEncoder
interface Flow\ETL\Adapter\CSV\CSVOpenSource
class Flow\ETL\Adapter\CSV\RustCSVEncoder implements Flow\ETL\Adapter\CSV\CSVEncoder
class Flow\ETL\Adapter\CSV\RustCSVOpenSource implements Flow\ETL\Adapter\CSV\CSVOpenSource
interface Flow\ETL\Adapter\JSON\JSONEncoder
interface Flow\ETL\Adapter\JSON\JsonOpenSource
class Flow\ETL\Adapter\JSON\RustJSONEncoder implements Flow\ETL\Adapter\JSON\JSONEncoder
class Flow\ETL\Adapter\JSON\RustJsonOpenSource implements Flow\ETL\Adapter\JSON\JsonOpenSource
interface Flow\ETL\Adapter\Parquet\ParquetOpenSink
interface Flow\ETL\Adapter\Parquet\ParquetOpenSource
class Flow\ETL\Adapter\Parquet\RustParquetOpenSink implements Flow\ETL\Adapter\Parquet\ParquetOpenSink
class Flow\ETL\Adapter\Parquet\RustParquetOpenSource implements Flow\ETL\Adapter\Parquet\ParquetOpenSource
interface Flow\ETL\Column\Backend
interface Flow\ETL\Column\Column
interface Flow\ETL\Column\ColumnBuilder
class Flow\ETL\Column\RustBackend implements Flow\ETL\Column\Backend
class Flow\ETL\Column\RustColumn implements Flow\ETL\Column\Column
class Flow\ETL\Column\RustColumnBuilder implements Flow\ETL\Column\ColumnBuilder
class Flow\ETL\Column\RustColumnsBatch implements 
class Flow\ETL\RustIterator implements Iterator
bool(true)
