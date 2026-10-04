--TEST--
RustIterator behaves as the Generator it replaced: it starts on first use, ends for good when its producer throws (valid() false, key() and current() null, next() a no-op) and refuses rewind() only after next()
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\JSON\RustJsonOpenSource;
use Flow\ETL\Column\PhpBackend;
use Flow\Filesystem\Stream\StringSourceStream;

use function Flow\ETL\DSL\{int_schema, schema};
use function Flow\Filesystem\DSL\path;

$schema = schema(int_schema('id'));
$id = static fn(mixed $rows): string => $rows === null ? 'null' : (string) $rows->toArray()[0]['id'];
$step = static function (string $label, callable $call): void {
    // the class differs on purpose: RustIterator throws flow's RuntimeException, a Generator \Exception
    try {
        $result = var_export($call(), true);
    } catch (Throwable) {
        $result = 'threw';
    }

    echo $label, ': ', $result, "\n";
};
$drive = static function (Iterator $iterator) use ($id, $step): void {
    $step('key before rewind', static fn() => $iterator->key());
    $step('current', static fn() => $id($iterator->current()));
    $step('rewind', static fn() => $iterator->rewind());
    $step('next', static fn() => $iterator->next());
    $step('current', static fn() => $id($iterator->current()));
    $step('next (producer throws)', static fn() => $iterator->next());
    $step('valid', static fn() => $iterator->valid());
    $step('key', static fn() => $iterator->key());
    $step('current', static fn() => $iterator->current());
    $step('next', static fn() => $iterator->next());
    $step('rewind', static fn() => $iterator->rewind());
};
$failingFirst = static function (Iterator $iterator) use ($step): void {
    $step('rewind (producer throws)', static fn() => $iterator->rewind());
    $step('valid', static fn() => $iterator->valid());
    $step('rewind', static fn() => $iterator->rewind());
};

$rows = static fn(int $id) => php_rows(schema(int_schema('id')), [['id' => $id]]);
$generator = (static function () use ($rows): Generator {
    yield $rows(1);
    yield $rows(2);
    throw new RuntimeException('malformed');
})();
$rust = (new RustJsonOpenSource(new StringSourceStream(path('memory://a.jsonl'), "{\"id\":1}\n{\"id\":2}\nnot json\n"), true, 'memory://a.jsonl', '', 0))
    ->batches($schema, 1, new PhpBackend());

ob_start();
$drive($generator);
$expected = ob_get_clean();
ob_start();
$drive($rust);
$actual = ob_get_clean();
echo $actual, $actual === $expected ? "identical\n" : "DIFFERENT, Generator:\n{$expected}";

$generator = (static function (): Generator {
    throw new RuntimeException('malformed');
    yield;
})();
$rust = (new RustJsonOpenSource(new StringSourceStream(path('memory://b.jsonl'), "not json\n"), true, 'memory://b.jsonl', '', 0))
    ->batches($schema, 1, new PhpBackend());

ob_start();
$failingFirst($generator);
$expected = ob_get_clean();
ob_start();
$failingFirst($rust);
$actual = ob_get_clean();
echo $actual, $actual === $expected ? "identical\n" : "DIFFERENT, Generator:\n{$expected}";
?>
--EXPECT--
key before rewind: 0
current: '1'
rewind: NULL
next: NULL
current: '2'
next (producer throws): threw
valid: false
key: NULL
current: NULL
next: NULL
rewind: threw
identical
rewind (producer throws): threw
valid: false
rewind: NULL
identical
