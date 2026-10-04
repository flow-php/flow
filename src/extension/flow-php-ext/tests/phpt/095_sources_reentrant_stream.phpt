--TEST--
RustCSVOpenSource and RustJsonOpenSource hold no borrow while their stream runs: a stream calling producedRows(), producedBytes() and close() back mid-read is answered, never a panic
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\RustCSVOpenSource;
use Flow\ETL\Adapter\JSON\RustJsonOpenSource;
use Flow\ETL\Column\PhpBackend;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Stream\StringSourceStream;

use function Flow\ETL\DSL\{int_schema, schema};
use function Flow\Filesystem\DSL\path;

// a stream that calls $hook before every chunk it hands out
final class HookedStream implements SourceStream
{
    public ?Closure $hook = null;

    private StringSourceStream $stream;

    public function __construct(string $content)
    {
        $this->stream = new StringSourceStream(path('memory://hooked'), $content);
    }

    public function close(): void
    {
        $this->stream->close();
    }

    public function content(): string
    {
        return $this->stream->content();
    }

    public function isOpen(): bool
    {
        return $this->stream->isOpen();
    }

    public function iterate(int $length = 1): Generator
    {
        foreach ($this->stream->iterate(4) as $chunk) {
            ($this->hook)();

            yield $chunk;
        }
    }

    public function path(): Path
    {
        return $this->stream->path();
    }

    public function read(int $length, int $offset): string
    {
        ($this->hook)();

        return $this->stream->read(4, $offset);
    }

    public function readLines(string $separator = "\n", ?int $length = null): Generator
    {
        return $this->stream->readLines($separator, $length);
    }

    public function size(): ?int
    {
        return $this->stream->size();
    }
}

$schema = schema(int_schema('id'));
$stream = new HookedStream("id\n1\n2\n3\n");
$csv = new RustCSVOpenSource($stream, ',', '"', '\\', true, true, true);
$seen = [];
$stream->hook = static function () use ($csv, &$seen): void {
    $seen[] = $csv->producedRows() . '/' . $csv->producedBytes();
};
$ids = [];

foreach ($csv->batches($schema, 1, new PhpBackend()) as $rows) {
    $ids[] = $rows->toArray()[0]['id'];
}

echo 'csv ids: ', json_encode($ids), ', counters answered mid-read: ', count($seen), ', after: ', $csv->producedRows(), "\n";

$stream = new HookedStream("{\"id\":1}\n{\"id\":2}\n");
$json = new RustJsonOpenSource($stream, true, 'memory://hooked', '', 0);
$closed = false;
$stream->hook = static function () use ($json, &$closed): void {
    if (!$closed) {
        $closed = true;
        $json->close();
    }
};

echo 'json close() mid-read: ', outcome(static fn() => count(iterator_to_array($json->batches($schema, 1, new PhpBackend())))), "\n";
echo 'json closed: ', var_export($closed, true), "\n";
?>
--EXPECT--
csv ids: [1,2,3], counters answered mid-read: 3, after: 3
json close() mid-read: i:2;
json closed: true
