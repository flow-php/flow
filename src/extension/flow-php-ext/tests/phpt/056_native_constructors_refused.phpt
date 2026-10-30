--TEST--
native columns and builders come only from RustBackend; neither a column nor a Rows of them is PHP-serializable, a Rows of them round-trips through FloeSerializer
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Storage\Floe\FloeSerializer;
use Flow\ETL\Values\Column\RustBackend;
use Flow\ETL\Values\Column\RustColumn;
use Flow\ETL\Values\Column\RustColumnBuilder;

use function Flow\Serializer\DSL\serialize_to_string;
use function Flow\Serializer\DSL\unserialize_from_string;

expect_exception(static fn() => new RustColumn());
expect_exception(static fn() => new RustColumnBuilder());

foreach ([RustColumn::class, RustColumnBuilder::class] as $class) {
    echo outcome(static fn() => (new ReflectionClass($class))->newInstanceWithoutConstructor()), "\n";
}

$rows = native_rows(all_types_schema(), all_types_values());

echo get_class($rows->column('int')), "\n";
echo outcome(static fn() => $rows->column('int')), "\n";
echo outcome(static fn() => serialize($rows)), "\n";
$serializer = new FloeSerializer(new RustBackend());
echo comparable(unserialize_from_string($serializer, serialize_to_string($serializer, $rows))->toArray()) === comparable($rows->toArray()) ? 'round trip' : 'FAIL', "\n";
?>
--EXPECT--
Flow\ETL\Foundation\Exception\RuntimeException: Flow\ETL\Values\Column\RustColumn is built by RustBackend
Flow\ETL\Foundation\Exception\RuntimeException: Flow\ETL\Values\Column\RustColumnBuilder is built by RustBackend
ReflectionException: Class Flow\ETL\Values\Column\RustColumn is an internal class marked as final that cannot be instantiated without invoking its constructor
ReflectionException: Class Flow\ETL\Values\Column\RustColumnBuilder is an internal class marked as final that cannot be instantiated without invoking its constructor
Flow\ETL\Values\Column\RustColumn
Exception: Serialization of 'Flow\ETL\Values\Column\RustColumn' is not allowed
Exception: Serialization of 'Flow\ETL\Values\Column\RustColumn' is not allowed
round trip
