--TEST--
RustCSVOpenSource rejects an invalid dialect, batch size and sniff row budget
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\IO\Extractor\RustCSVOpenSource;
use Flow\ETL\Values\Column\RustBackend;
use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Types\Type\Native\String\StringTypeNarrower;

use function Flow\ETL\DSL\{int_schema, schema};

$source = static fn(string $separator = ',', string $enclosure = '"', string $escape = '\\'): RustCSVOpenSource => new RustCSVOpenSource(
    new MemorySourceStream("id\n1\n"),
    $separator,
    $enclosure,
    $escape,
    true,
    true,
    true,
);

expect_exception(static fn() => $source(',,'));
expect_exception(static fn() => $source(',', ''));
expect_exception(static fn() => $source(',', '"', '\\\\'));
expect_exception(static fn() => $source()->batches(schema(int_schema('id')), 0, new RustBackend()));
expect_exception(static fn() => $source()->batches(schema(int_schema('id')), -1, new RustBackend()));
expect_exception(static fn() => $source()->sniff(['id'], -2, new StringTypeNarrower()));
?>
--EXPECT--
Flow\ETL\Foundation\Exception\RuntimeException: flow_php CSV separator must be exactly one byte
Flow\ETL\Foundation\Exception\RuntimeException: flow_php CSV enclosure must be exactly one byte
Flow\ETL\Foundation\Exception\RuntimeException: flow_php CSV escape must be empty or exactly one byte
Flow\ETL\Foundation\Exception\RuntimeException: flow_php CSV batch size must be greater than 0
Flow\ETL\Foundation\Exception\RuntimeException: flow_php CSV batch size must be greater than 0
Flow\ETL\Foundation\Exception\InvalidArgumentException: Row budget must be 0 or more. Use null for every row.
