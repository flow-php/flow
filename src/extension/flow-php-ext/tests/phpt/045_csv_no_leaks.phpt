--TEST--
repeated native CSV reading does not leak memory
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\RustCSVReaderNative;

$raw = "\xEF\xBB\xBFid,name,5,id\n1,\"multi\nline\",x,\n2,,\"a\"\"b\",y\n\n3\n";

$cycle = static function () use ($raw): void {
    $reader = new RustCSVReaderNative(',', '"', '\\', true, true, true);

    foreach (str_split($raw, 5) as $chunk) {
        $reader->feed($chunk);
        $reader->next(2);
    }

    $reader->finish();
    $reader->headers();

    while ($reader->next(2) !== []) {
    }

    $unfinished = new RustCSVReaderNative(';', "'", '', false, false, false);
    $unfinished->feed("a;'open\n");
    $unfinished->next(10);

    try {
        new RustCSVReaderNative(',,', '"', '\\', true, true, true);
        throw new LogicException('an invalid separator was accepted');
    } catch (Flow\Floe\Exception\ExtensionException) {
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
