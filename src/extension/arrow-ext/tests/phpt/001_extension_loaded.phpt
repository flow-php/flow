--TEST--
arrow extension is loaded and the Parquet classes are registered
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
var_dump(extension_loaded("arrow"));

foreach (['Flow\Parquet\Engine\RustParquetEngine', 'Flow\Parquet\Engine\RustParquetFileReader', 'Flow\Arrow\Parquet\RustColumnsReader', 'Flow\Parquet\Engine\RustParquetFileWriter'] as $class) {
    $r = new ReflectionClass($class);
    echo $class, ': ', $r->isInternal() ? 'internal' : 'userland', ', ', $r->isFinal() ? 'final' : 'open', "\n";
}

foreach (['Flow\Arrow\Parquet\Reader', 'Flow\Arrow\Parquet\Writer', 'Flow\Arrow\Parquet\Exception'] as $class) {
    var_dump(class_exists($class, false));
}
?>
--EXPECT--
bool(true)
Flow\Parquet\Engine\RustParquetEngine: internal, final
Flow\Parquet\Engine\RustParquetFileReader: internal, final
Flow\Arrow\Parquet\RustColumnsReader: internal, final
Flow\Parquet\Engine\RustParquetFileWriter: internal, final
bool(false)
bool(false)
bool(false)
