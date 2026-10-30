--TEST--
RustBackend describes itself and rebuilds, and RustColumnBuilder::append() refuses a value as PhpBackend's builder does
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Column\RustBackend;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Json;
use Flow\ETL\Foundation\Describe\Rebuild;

use function Flow\ETL\DSL\int_schema;

$backend = new RustBackend();
var_dump($backend instanceof Describable);
var_dump((new Rebuild())->of(Json::decode(Json::encode($backend->describe()->normalize())), RustBackend::class) instanceof RustBackend);

foreach ([new RustBackend(), new PhpBackend()] as $lane) {
    try {
        $lane->builder(int_schema('a'))->append('abc');
    } catch (Throwable $e) {
        echo $e::class, ': ', $e->getMessage(), "\n";
    }
}
?>
--EXPECT--
bool(true)
bool(true)
Flow\ETL\Foundation\Exception\ColumnMismatchException: Row does not match its schema: column "a": could not convert 'abc' (string) to integer
Flow\ETL\Foundation\Exception\ColumnMismatchException: Row does not match its schema: column "a": could not convert 'abc' (string) to integer
