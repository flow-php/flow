--TEST--
native columns and builders come only from DefaultBackend; a column is not serializable, a Rows of them is
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Column\NativeColumn;
use Flow\ETL\Column\NativeColumnBuilder;

expect_exception(static fn() => new NativeColumn());
expect_exception(static fn() => new NativeColumnBuilder());

foreach ([NativeColumn::class, NativeColumnBuilder::class] as $class) {
    echo outcome(static fn() => (new ReflectionClass($class))->newInstanceWithoutConstructor()), "\n";
}

$rows = native_rows(all_types_schema(), all_types_values());

echo get_class($rows->column('int')), "\n";
echo outcome(static fn() => $rows->column('int')), "\n";
echo comparable(unserialize(serialize($rows))->toArray()) === comparable($rows->toArray()) ? 'round trip' : 'FAIL', "\n";
?>
--EXPECT--
Flow\Floe\Exception\ExtensionException: Flow\ETL\Column\NativeColumn is built by DefaultBackend
Flow\Floe\Exception\ExtensionException: Flow\ETL\Column\NativeColumnBuilder is built by DefaultBackend
ReflectionException: Class Flow\ETL\Column\NativeColumn is an internal class marked as final that cannot be instantiated without invoking its constructor
ReflectionException: Class Flow\ETL\Column\NativeColumnBuilder is an internal class marked as final that cannot be instantiated without invoking its constructor
Flow\ETL\Column\NativeColumn
Exception: Serialization of 'Flow\ETL\Column\NativeColumn' is not allowed
round trip
