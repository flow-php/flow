--TEST--
RustBackend::adopt() refuses an untyped (mixed) column with the PHP wording
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Values\Column\RustBackend;
use Flow\ETL\Values\Column\ValueColumn;
use Flow\ETL\Values\Column\PhpBackend;
use function Flow\ETL\DSL\int_schema;

echo outcome(static fn() => (new RustBackend())->adopt(int_schema('a'), new ValueColumn([1, null]))), "\n";
$typed = (new PhpBackend())->constant(int_schema('a', nullable: true), 1, 2);

echo get_class((new RustBackend())->adopt(int_schema('a', nullable: true), $typed)), "\n";
?>
--EXPECT--
Flow\ETL\Foundation\Exception\ColumnMismatchException: Row does not match its schema: column "a": mixed cannot be a batch column, an untyped function result exists only inside function evaluation
Flow\ETL\Values\Column\RustColumn
