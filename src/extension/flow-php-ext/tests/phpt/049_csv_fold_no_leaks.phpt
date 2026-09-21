--TEST--
repeated native CSV folding - including every PHP callback and a rejected time zone offset - does not leak memory
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\RustColumnFoldNative;
use Flow\ETL\Adapter\CSV\RustCSVReaderNative;
use Flow\ETL\Schema\Inference\InferredTypes;

$candidates = array_map(static fn($type): string => $type->toString(), InferredTypes::default()->toArray());
$raw = "id,json,at,zone,offset,dup,dup\n1,\"{\"\"a\"\":1}\",2024-01-01 10:00:00,Europe/Warsaw,+99:60,x,\n2,[1],2024-02-30,UTC,+02:00,,y\n";

$cycle = static function () use ($candidates, $raw): void {
    $reader = new RustCSVReaderNative(',', '"', '\\', true, true, true);
    $reader->feed($raw);
    $reader->finish();
    $fold = new RustColumnFoldNative(['id', 'json'], $candidates);
    $reader->fold($fold, -1);
    $fold->types();
    $fold->rows();
    $fold->narrowOne('+99:60');
    $fold->narrowOne('{"a":[1,2]}');
    $fold->narrowOne('2024-01-01');

    foreach (json_leak_cells() as $cell) {
        $fold->narrowOne($cell);
    }
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
