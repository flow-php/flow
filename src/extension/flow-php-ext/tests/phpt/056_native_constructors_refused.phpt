--TEST--
native columns and builders come only from RustBackend; a column is not serializable, a Rows of them is
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Column\RustColumn;
use Flow\ETL\Column\RustColumnBuilder;

expect_exception(static fn() => new RustColumn());
expect_exception(static fn() => new RustColumnBuilder());

foreach ([RustColumn::class, RustColumnBuilder::class] as $class) {
    echo outcome(static fn() => (new ReflectionClass($class))->newInstanceWithoutConstructor()), "\n";
}

$rows = native_rows(all_types_schema(), all_types_values());

echo get_class($rows->column('int')), "\n";
echo outcome(static fn() => $rows->column('int')), "\n";
echo comparable(unserialize(serialize($rows))->toArray()) === comparable($rows->toArray()) ? 'round trip' : 'FAIL', "\n";
?>
--EXPECT--
Flow\ETL\Exception\RuntimeException: Flow\ETL\Column\RustColumn is built by RustBackend
Flow\ETL\Exception\RuntimeException: Flow\ETL\Column\RustColumnBuilder is built by RustBackend
ReflectionException: Class Flow\ETL\Column\RustColumn is an internal class marked as final that cannot be instantiated without invoking its constructor
ReflectionException: Class Flow\ETL\Column\RustColumnBuilder is an internal class marked as final that cannot be instantiated without invoking its constructor
Flow\ETL\Column\RustColumn
Exception: Serialization of 'Flow\ETL\Column\RustColumn' is not allowed
round trip
