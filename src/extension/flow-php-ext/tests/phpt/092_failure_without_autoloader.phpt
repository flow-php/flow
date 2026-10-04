--TEST--
without an autoloader flow_php fails with a plain Exception, never recursing on the missing RuntimeException class
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
foreach ([
    static fn() => new Flow\ETL\Adapter\CSV\RustCSVOpenSource(new stdClass(), ',,', '"', '\\', true, true, true),
    static fn() => new Flow\ETL\Column\RustColumn(),
] as $fn) {
    try {
        $fn();
        echo "FAIL: no exception thrown\n";
    } catch (Exception $e) {
        echo get_class($e), ': ', $e->getMessage(), "\n";
    }
}

var_dump(class_exists('Flow\ETL\Exception\RuntimeException', false));
?>
--EXPECT--
Exception: flow_php CSV separator must be exactly one byte
Exception: Flow\ETL\Column\RustColumn is built by RustBackend
bool(false)
