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
interface Flow\ETL\Adapter\CSV\IO\Extractor\CSVOpenSource
class Flow\ETL\Adapter\CSV\IO\Extractor\RustCSVOpenSource implements Flow\ETL\Adapter\CSV\IO\Extractor\CSVOpenSource
interface Flow\ETL\Adapter\CSV\IO\Loader\CSVEncoder
class Flow\ETL\Adapter\CSV\IO\Loader\RustCSVEncoder implements Flow\ETL\Adapter\CSV\IO\Loader\CSVEncoder
interface Flow\ETL\Adapter\JSON\IO\Extractor\JsonOpenSource
class Flow\ETL\Adapter\JSON\IO\Extractor\RustJsonOpenSource implements Flow\ETL\Adapter\JSON\IO\Extractor\JsonOpenSource
interface Flow\ETL\Adapter\JSON\IO\Loader\JsonEncoder
class Flow\ETL\Adapter\JSON\IO\Loader\RustJsonEncoder implements Flow\ETL\Adapter\JSON\IO\Loader\JsonEncoder
interface Flow\ETL\Adapter\Parquet\IO\Extractor\ParquetOpenSource
class Flow\ETL\Adapter\Parquet\IO\Extractor\RustParquetOpenSource implements Flow\ETL\Adapter\Parquet\IO\Extractor\ParquetOpenSource
class Flow\ETL\Adapter\Parquet\IO\Loader\RustParquetFileWriter implements Flow\ETL\IO\Loader\File\FileWriter
interface Flow\ETL\Foundation\Describe\Describable
interface Flow\ETL\IO\Loader\File\FileWriter
interface Flow\ETL\Values\Column\Backend
interface Flow\ETL\Values\Column\Column
interface Flow\ETL\Values\Column\ColumnBuilder
class Flow\ETL\Values\Column\RustBackend implements Flow\ETL\Values\Column\Backend, Flow\ETL\Foundation\Describe\Describable
class Flow\ETL\Values\Column\RustColumn implements Flow\ETL\Values\Column\Column
class Flow\ETL\Values\Column\RustColumnBuilder implements Flow\ETL\Values\Column\ColumnBuilder
class Flow\ETL\Values\Column\RustColumnsBatch implements 
class Flow\ETL\Values\RustIterator implements Iterator
bool(true)
