--TEST--
arrow extension is loaded and the Parquet and Arrow C Data classes are registered
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
var_dump(extension_loaded("arrow"));

foreach (['Flow\Arrow\Parquet\ParquetFile', 'Flow\Arrow\Parquet\ColumnsReader', 'Flow\Arrow\Parquet\RowsWriter', 'Flow\Arrow\Parquet\BatchReader', 'Flow\Arrow\ArrowBatch', 'Flow\Arrow\ArrowSchema'] as $class) {
    $r = new ReflectionClass($class);
    echo $class, ': ', $r->isInternal() ? 'internal' : 'userland', ', ', $r->isFinal() ? 'final' : 'open', "\n";
}

foreach (['Flow\Arrow\Parquet\Reader', 'Flow\Arrow\Parquet\Writer', 'Flow\Arrow\Parquet\Exception'] as $class) {
    var_dump(class_exists($class, false));
}

foreach (['Flow\Arrow\ArrowBatch', 'Flow\Arrow\ArrowSchema'] as $class) {
    try {
        new $class();
    } catch (Throwable $e) {
        echo $class, ': ', $e->getMessage(), "\n";
    }
}
?>
--EXPECT--
bool(true)
Flow\Arrow\Parquet\ParquetFile: internal, final
Flow\Arrow\Parquet\ColumnsReader: internal, final
Flow\Arrow\Parquet\RowsWriter: internal, final
Flow\Arrow\Parquet\BatchReader: internal, final
Flow\Arrow\ArrowBatch: internal, final
Flow\Arrow\ArrowSchema: internal, final
bool(false)
bool(false)
bool(false)
Flow\Arrow\ArrowBatch: You cannot instantiate this class from PHP.
Flow\Arrow\ArrowSchema: You cannot instantiate this class from PHP.
