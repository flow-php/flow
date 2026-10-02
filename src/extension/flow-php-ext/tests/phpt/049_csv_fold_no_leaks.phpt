--TEST--
repeated native CSV folding - including every PHP callback and a rejected time zone offset - does not leak memory
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\RustCSVOpenSource;
use Flow\ETL\Schema\Inference\InferredTypes;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Types\Type\Native\String\StringTypeNarrower;

$typer = new StringTypeNarrower(InferredTypes::default()->toArray());
$raw = "id,json,at,zone,offset,dup,dup\n1,\"{\"\"a\"\":1}\",2024-01-01 10:00:00,Europe/Warsaw,+99:60,x,\n2,[1],2024-02-30,UTC,+02:00,,y\n";

$cycle = static function () use ($typer, $raw): void {
    (new RustCSVOpenSource(new MemorySourceStream($raw), ',', '"', '\\', true, true, true))
        ->sniff(['id', 'json'], -1, new SchemaInference(), $typer);
    rust_sniff_column(['+99:60', '{"a":[1,2]}', '2024-01-01', ...json_leak_cells()], $typer);
};

for ($i = 0; $i < 10; $i++) {
    $cycle();
}
gc_collect_cycles();
$baseline = memory_get_usage(false);

for ($i = 0; $i < 10000; $i++) {
    $cycle();
}
gc_collect_cycles();

var_dump(memory_get_usage(false) <= $baseline);
?>
--EXPECT--
bool(true)
