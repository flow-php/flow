--TEST--
repeated native CSV reading does not leak memory
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\RustCSVOpenSource;
use Flow\Filesystem\Stream\MemorySourceStream;

$raw = "\xEF\xBB\xBFid,name,5,id\n1,\"multi\nline\",x,\n2,,\"a\"\"b\",y\n\n3\n";

$cycle = static function () use ($raw): void {
    $source = new RustCSVOpenSource(new MemorySourceStream($raw), ',', '"', '\\', true, true, true, 5);

    foreach ($source->records() as $record) {
    }

    $source->headers();

    // abandoned after the first record
    $unfinished = (new RustCSVOpenSource(new MemorySourceStream("a;'open\nb;c\n"), ';', "'", '', false, false, false, 4))->records();
    $unfinished->rewind();

    try {
        new RustCSVOpenSource(new MemorySourceStream($raw), ',,', '"', '\\', true, true, true);
        throw new LogicException('an invalid separator was accepted');
    } catch (Flow\ETL\Exception\RuntimeException) {
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
