--TEST--
arrow extension is loaded and the Parquet and Arrow C Data classes are registered
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
var_dump(extension_loaded("arrow"));

foreach (['Flow\Parquet\Engine\RustParquetEngine', 'Flow\Parquet\Engine\RustParquetFileReader', 'Flow\Arrow\Parquet\RustColumnsReader', 'Flow\Parquet\Engine\RustParquetFileWriter', 'Flow\Arrow\Parquet\RustBatchReader', 'Flow\Arrow\RustParquetBatch', 'Flow\Arrow\RustArrowSchema'] as $class) {
    $r = new ReflectionClass($class);
    echo $class, ': ', $r->isInternal() ? 'internal' : 'userland', ', ', $r->isFinal() ? 'final' : 'open', "\n";
}

foreach (['Flow\Arrow\Parquet\Reader', 'Flow\Arrow\Parquet\Writer', 'Flow\Arrow\Parquet\Exception'] as $class) {
    var_dump(class_exists($class, false));
}

foreach (['Flow\Arrow\RustParquetBatch', 'Flow\Arrow\RustArrowSchema'] as $class) {
    try {
        new $class();
    } catch (Throwable $e) {
        echo $class, ': ', $e->getMessage(), "\n";
    }
}
?>
--EXPECT--
bool(true)
Flow\Parquet\Engine\RustParquetEngine: internal, final
Flow\Parquet\Engine\RustParquetFileReader: internal, final
Flow\Arrow\Parquet\RustColumnsReader: internal, final
Flow\Parquet\Engine\RustParquetFileWriter: internal, final
Flow\Arrow\Parquet\RustBatchReader: internal, final
Flow\Arrow\RustParquetBatch: internal, final
Flow\Arrow\RustArrowSchema: internal, final
bool(false)
bool(false)
bool(false)
Flow\Arrow\RustParquetBatch: You cannot instantiate this class from PHP.
Flow\Arrow\RustArrowSchema: You cannot instantiate this class from PHP.
