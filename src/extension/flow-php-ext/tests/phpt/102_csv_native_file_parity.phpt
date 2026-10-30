--TEST--
A local CSV file read by Rust with pread (NativeLocalSourceStream, and every RangeSourceStream window over it) yields the records, batches and producedBytes() the PHP-fed arm yields; the native arm's IO volume is not pinned (NativeLocalSourceStream is final, no counting hook)
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\IO\Extractor\RustCSVOpenSource;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Column\RustBackend;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Flow\Filesystem\Stream\RangeSourceStream;
use Flow\ETL\Values\Column\Frame\NoopCodec;

use function Flow\ETL\DSL\{schema, str_schema};
use function Flow\Filesystem\DSL\path;

$raws = [
    "\xEF\xBB\xBFid,name,note\r\n1,\"multi\r\nline\",\"a\\\"b\"\n2,\"x\"\"y\",  \"z\"\n\n3,plain,\"open\nat eof",
    "\xEF\xBB\xBF\"a\",b\n1,2\n",
    "\xEF\xBB\xBF\"a\nb\",c\n1,2\n",
    "a, \"b\nc\",d\ne\n",
    "h1,h2\na\r,b\n",
    "id\n1\n",
    "id\n1\nx\n",
    "a,b\nx,y\n",
    "id,name\n1,a\n2,\n",
    "id,name,name\n1,a,b\n2,c,d\n",
    "id,name\n1\n2,b,extra\n",
    "1,2\na,3\n",
    "id,name\n1,a\n2,b\n3,c\nx,d\n",
    "id\n",
    "",
];

foreach (csv_narrow_corpus() as $cell) {
    $raws[] = "v\n\"" . str_replace('"', '""', $cell) . "\"\n";
}

$file = tempnam(sys_get_temp_dir(), 'phpt102');
$source = static fn(SourceStream $stream): RustCSVOpenSource => new RustCSVOpenSource($stream, ',', '"', '\\', true, true, true, 7, null);
$read = static fn(SourceStream $stream): string => outcome(static function () use ($stream, $source): array {
    $open = $source($stream);
    $records = iterator_to_array($open->records(), false);

    return [$open->headers(), $records, $open->producedBytes()];
});
$batches = static fn(SourceStream $stream, int $batchSize): string => outcome(static function () use ($stream, $source, $batchSize): array {
    $open = $source($stream);
    $headers = $open->headers();
    $frames = [];

    foreach ($open->batches(schema(...array_map(static fn(string $name) => str_schema($name, nullable: true), $headers)), $batchSize, new RustBackend()) as $batch) {
        $frames[] = bin2hex((new PhpBackend())->encodeFrame($batch, new NoopCodec()));
    }

    return $frames;
});

$identical = 0;
$compared = 0;

foreach ($raws as $raw) {
    file_put_contents($file, $raw);
    $len = strlen($raw);

    $php = $read(chunked_source_stream($raw, 7));
    $native = $read(NativeLocalSourceStream::open(path($file)));
    $compared++;

    if ($php === $native) {
        $identical++;
    } else {
        echo 'whole ', json_encode($raw), "\n  php:    {$php}\n  native: {$native}\n";
    }

    foreach ([1, 7, 100] as $batchSize) {
        $php = $batches(chunked_source_stream($raw, 7), $batchSize);
        $native = $batches(NativeLocalSourceStream::open(path($file)), $batchSize);
        $compared++;

        if ($php === $native) {
            $identical++;
        } else {
            echo 'batches ', $batchSize, ' ', json_encode($raw), "\n  php:    {$php}\n  native: {$native}\n";
        }
    }

    $bounds = array_values(array_unique(array_filter([0, 1, 2, 7, intdiv($len, 2), $len - 1, $len], static fn(int $at): bool => $at >= 0 && $at <= $len)));

    foreach ($bounds as $start) {
        foreach ($bounds as $end) {
            if ($end < $start) {
                continue;
            }

            $php = $read(new RangeSourceStream(chunked_source_stream($raw, 7), $start, $end));
            $native = $read(new RangeSourceStream(NativeLocalSourceStream::open(path($file)), $start, $end));
            $compared++;

            if ($php === $native) {
                $identical++;
            } else {
                echo "window [{$start}, {$end}) ", json_encode($raw), "\n  php:    {$php}\n  native: {$native}\n";
            }
        }
    }
}

unlink($file);
echo count($raws), " files: {$identical} of {$compared} reads identical\n";
?>
--EXPECT--
406 files: 12096 of 12096 reads identical
