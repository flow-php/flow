--TEST--
RustBackend::decodeFrame() points the Arrow buffers into the body string: native allocations grow by the widened offsets only, the rows outlive writes to and the release of the body, nothing leaks, and the body is released before the engine shuts down
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Column\RustBackend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Column\Frame\NoopCodec;

use function Flow\ETL\DSL\{float_schema, int_schema, schema, str_schema};

const ROWS = 100_000;
const TINY_FRAME = "\x02\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x18\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\x07\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00";

$schema = schema(int_schema('int'), float_schema('float'), str_schema('str'));
$backend = new RustBackend();
$values = [];

for ($i = 0; $i < ROWS; $i++) {
    $values[] = ['int' => $i, 'float' => $i / 2, 'str' => str_pad((string) $i, 32, 'x')];
}

$body = (new PhpBackend())->encodeFrame(php_rows($schema, $values), new NoopCodec());
$values = null;
$bodyBytes = strlen($body);
echo 'body over 4 MiB: ', var_export($bodyBytes > 4 * 1024 * 1024, true), "\n";

$before = $backend->allocatedBytes();
$rows = $backend->decodeFrame($body, $schema, new NoopCodec());
$grown = $backend->allocatedBytes() - $before;
echo 'native allocations grew by the widened offsets only: ', var_export($grown < ROWS * 8 * 1.1, true), "\n";
echo 'rows: ', $rows->count(), "\n";

$body[0] = 'x';
$body .= 'y';
echo 'first row after a write to the body: ', json_encode($rows->slice(0, 1)->toArray()), "\n";
echo 'last row after a write to the body: ', json_encode($rows->slice(ROWS - 1, 1)->toArray()), "\n";

unset($body);
gc_collect_cycles();
echo 'last row after the body is gone: ', json_encode($rows->slice(ROWS - 1, 1)->toArray()), "\n";

$tiny = $backend->decodeFrame(TINY_FRAME, schema(int_schema('id')), new NoopCodec());
echo 'interned body: ', json_encode($tiny->toArray()), "\n";

$cycle = static function () use ($backend, $schema): void {
    $body = (new PhpBackend())->encodeFrame(php_rows($schema, [['int' => 1, 'float' => 1.5, 'str' => 'a'], ['int' => 2, 'float' => 2.5, 'str' => 'bb']]), new NoopCodec());
    $rows = $backend->decodeFrame($body, $schema, new NoopCodec());
    $body = 'overwritten';
    $rows->column('str')->value(1);
};

for ($i = 0; $i < 10; $i++) {
    $cycle();
}
gc_collect_cycles();
$baseline = memory_get_usage(false);
$baselineRust = $backend->allocatedBytes();

for ($i = 0; $i < 100; $i++) {
    $cycle();
}
gc_collect_cycles();

echo 'php memory back at the baseline: ', var_export(memory_get_usage(false) <= $baseline, true), "\n";
echo 'native allocations back at the baseline: ', var_export($backend->allocatedBytes() === $baselineRust, true), "\n";

$kept = $backend->decodeFrame((new PhpBackend())->encodeFrame(php_rows($schema, [['int' => 1, 'float' => 1.5, 'str' => 'a']]), new NoopCodec()), $schema, new NoopCodec());
$cyclic = $backend->decodeFrame((new PhpBackend())->encodeFrame(php_rows($schema, [['int' => 2, 'float' => 2.5, 'str' => 'b']]), new NoopCodec()), $schema, new NoopCodec());
$holder = new stdClass();
$holder->rows = $cyclic;
$holder->self = static fn(): Rows => $holder->rows;
unset($cyclic);
echo "ok\n";
?>
--EXPECT--
body over 4 MiB: true
native allocations grew by the widened offsets only: true
rows: 100000
first row after a write to the body: [{"int":0,"float":0,"str":"0xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}]
last row after a write to the body: [{"int":99999,"float":49999.5,"str":"99999xxxxxxxxxxxxxxxxxxxxxxxxxxx"}]
last row after the body is gone: [{"int":99999,"float":49999.5,"str":"99999xxxxxxxxxxxxxxxxxxxxxxxxxxx"}]
interned body: [{"id":7},{"id":8}]
php memory back at the baseline: true
native allocations back at the baseline: true
ok
