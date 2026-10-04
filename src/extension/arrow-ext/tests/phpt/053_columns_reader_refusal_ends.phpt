--TEST--
RustColumnsReader behaves as a Generator: it starts on first use, a refused chunk ends it for good (valid() false, key() and current() null, next() a no-op) and rewind() is refused only after next()
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Arrow\Parquet\RustColumnsReader;
use Flow\Filesystem\Stream\StringSourceStream;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\RustParquetFileReader;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Writer;

use function Flow\Filesystem\DSL\{memory_filesystem, path};

// two row groups of two rows; the second's data pages overwritten, its footer entry intact
$filesystem = memory_filesystem();
$options = Options::default()->set(Option::ROW_GROUP_SIZE_BYTES, 1)->set(Option::ROW_GROUP_SIZE_CHECK_INTERVAL, 2);
$writer = new Writer(options: $options, engine: new PhpParquetEngine(options: $options));
$writer->openForStream($filesystem->writeTo(path('memory://groups.parquet')), Schema::with(FlatColumn::int64('id')));
$writer->writeBatch([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]]);
$writer->close();

$bytes = $filesystem->readFrom(path('memory://groups.parquet'))->content();
$open = static fn(string $bytes): RustParquetFileReader => new RustParquetFileReader(new StringSourceStream(path('memory://groups.parquet'), $bytes));
$chunk = $open($bytes)->metadata()->rowGroups()->all()[1]->columnChunks()[0];
$corrupt = substr_replace($bytes, str_repeat("\xFF", $chunk->totalCompressedSize()), $chunk->pageOffset(), $chunk->totalCompressedSize());

$step = static function (string $label, callable $call): void {
    try {
        $result = json_encode($call());
    } catch (Throwable) {
        $result = 'threw';
    }

    echo $label, ': ', $result, "\n";
};

$reader = new RustColumnsReader($open($corrupt), ['id'], 2, null, null);
$step('key before rewind', static fn() => $reader->key());
$step('current', static fn() => $reader->current());
$step('rewind', static fn() => $reader->rewind());
$step('next (chunk refused)', static fn() => $reader->next());
$step('valid', static fn() => $reader->valid());
$step('key', static fn() => $reader->key());
$step('current', static fn() => $reader->current());
$step('next', static fn() => $reader->next());
$step('rewind', static fn() => $reader->rewind());

// refused before the first chunk: rewind() throws once, then the reader is empty and rewinds freely
$corrupt = substr_replace($bytes, str_repeat("\xFF", 8), 4, 8);
$reader = new RustColumnsReader($open($corrupt), ['id'], 2, null, null);
$step('rewind (chunk refused)', static fn() => $reader->rewind());
$step('valid', static fn() => $reader->valid());
$step('rewind', static fn() => $reader->rewind());
?>
--EXPECT--
key before rewind: 0
current: {"id":[1,2]}
rewind: null
next (chunk refused): threw
valid: false
key: null
current: null
next: null
rewind: threw
rewind (chunk refused): threw
valid: false
rewind: null
