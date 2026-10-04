--TEST--
flow_php registers FLOW_PHP_ABI equal to the ABI flow-php/etl expects, so the Adaptive picks take the Rust lane
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Column\RustColumn;
use Flow\ETL\FlowPhpExtension;

use function Flow\ETL\DSL\int_schema;

var_dump(FLOW_PHP_ABI === FlowPhpExtension::ABI);
var_dump(FlowPhpExtension::detect()->available());
var_dump((new AdaptiveBackend())->constant(int_schema('id'), 1, 2) instanceof RustColumn);
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
