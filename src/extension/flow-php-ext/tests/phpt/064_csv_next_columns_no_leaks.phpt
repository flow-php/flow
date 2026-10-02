--TEST--
RustCSVOpenSource::batches() leaks neither PHP memory nor native allocations
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\RustCSVOpenSource;
use Flow\ETL\Column\RustBackend;
use Flow\Filesystem\Stream\MemorySourceStream;

use function Flow\ETL\DSL\{datetime_schema, int_schema, schema, str_schema};

$raw = "\xEF\xBB\xBFid,name,at,extra\n1,\"multi\nline\",2026-01-02T03:04:05Z,\n2,,2026-01-02T03:04:05.5Z,y\n\n3\n";
$schema = schema(int_schema('id', nullable: true), str_schema('name', nullable: true), datetime_schema('at', nullable: true));
$refusing = schema(int_schema('name'));

$cycle = static function () use ($raw, $schema, $refusing): void {
    foreach ((new RustCSVOpenSource(new MemorySourceStream($raw), ',', '"', '\\', true, true, true, 5))->batches($schema, 2, new RustBackend()) as $batch) {
    }

    try {
        foreach ((new RustCSVOpenSource(new MemorySourceStream("name\nx\n"), ',', '"', '\\', true, true, true))->batches($refusing, 2, new RustBackend()) as $batch) {
        }

        throw new LogicException('a refused cell was accepted');
    } catch (Flow\ETL\Exception\SchemaMismatchException) {
    }
};

for ($i = 0; $i < 10; $i++) {
    $cycle();
}
gc_collect_cycles();
$baseline = memory_get_usage(false);
$baselineRust = (new RustBackend())->allocatedBytes();

for ($i = 0; $i < 1000; $i++) {
    $cycle();
}
gc_collect_cycles();

var_dump(memory_get_usage(false) <= $baseline);
var_dump((new RustBackend())->allocatedBytes() === $baselineRust);
?>
--EXPECT--
bool(true)
bool(true)
