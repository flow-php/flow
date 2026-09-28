--TEST--
flow_php extension is loaded and symbols are registered
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
var_dump(extension_loaded("flow_php"));

foreach (['Backend', 'Column', 'ColumnBuilder'] as $interface) {
    var_dump(interface_exists('Flow\ETL\Column\\' . $interface, false));
}

foreach (['Flow\ETL\Column\DefaultBackend', 'Flow\ETL\Column\NativeColumn', 'Flow\ETL\Column\NativeColumnBuilder', 'Flow\ETL\Adapter\CSV\RustCSVReaderNative', 'Flow\ETL\Adapter\CSV\RustColumnFoldNative', 'Flow\Floe\Exception\ExtensionException'] as $class) {
    var_dump(class_exists($class, false));
}
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
