--TEST--
RustParquetFileReader implements ParquetFileReader and RustParquetFileWriter implements ParquetFileWriter: the lib's metadata and schema, RustColumnsReader as an Iterator that streams once, INT96 and LZO refusals, closed reader and writer texts
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Parquet\Engine\RustParquetFileReader;
use Flow\Parquet\Engine\RustParquetFileWriter;
use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Flow\Filesystem\Tests\Double\FailingAppendDestinationStream;
use Flow\Filesystem\Tests\Double\FailingCloseDestinationStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Engine\Arrow\OptionsConverter;
use Flow\Parquet\Engine\Arrow\SchemaConverter;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\ParquetFileWriter;
use Flow\Parquet\Reader;

use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;

$fixtures = realpath(__DIR__ . '/../../../../lib/parquet/tests/Flow/Parquet/Tests/Integration/IO/Fixtures');
$open = static fn(string $name, bool $int96AsDatetime = true): RustParquetFileReader => new RustParquetFileReader(
    NativeLocalSourceStream::open(path_real("{$fixtures}/{$name}")),
    $int96AsDatetime,
);

echo "-- reader\n";
$file = $open('structs.parquet');
$metadata = $file->metadata();
$bytes = array_sum(array_map(static fn($group): int => $group->totalByteSize(), $metadata->rowGroups()->all()));
var_dump($file instanceof ParquetFileReader);
var_dump(serialize($metadata) === serialize(Reader::php()->read("{$fixtures}/structs.parquet")->metadata()));
var_dump(serialize($open('structs.parquet')->schema()) === serialize($metadata->schema()), $file->schema() === $metadata->schema());
var_dump($file->rowsNumber() === $metadata->rowsNumber(), $file->totalByteSize() === $bytes);

$chunks = $file->readColumns(['struct_flat', 'struct_flat.int'], 30, 70, 5);
$php = (new PhpParquetEngine(ByteOrder::LITTLE_ENDIAN, new Options()))
    ->openForRead(NativeLocalSourceStream::open(path_real("{$fixtures}/structs.parquet")))
    ->readColumns(['struct_flat', 'struct_flat.int'], 30, 70, 5);
var_dump($chunks instanceof Iterator);
$read = [];

foreach ($chunks as $key => $chunk) {
    $read[$key] = $chunk;
}

echo json_encode(array_map(static fn(array $chunk): int => count($chunk['struct_flat.int']), $read)), "\n";
var_dump($read == iterator_to_array($php));
echo arrow_outcome(static fn() => $chunks->rewind()), "\n";
echo arrow_outcome(static fn() => iterator_to_array($open('EdgeCases/int96.parquet', false)->readColumns(['ts'], 10, null, null))), "\n";
var_dump(count(iterator_to_array($open('EdgeCases/int96.parquet')->readColumns(['ts'], 10, null, null))) > 0);
echo arrow_outcome(static fn() => $open('lists.parquet')->readColumns(['list.list.element'], 10, null, null)), "\n";
echo arrow_outcome(static fn() => new RustParquetFileReader(new MemorySourceStream('{"not": "parquet"}'))), "\n";

$file->close();

foreach ([
    static fn() => $file->metadata(),
    static fn() => $file->schema(),
    static fn() => $file->rowsNumber(),
    static fn() => $file->totalByteSize(),
    static fn() => $file->thrift(),
    static fn() => $file->readColumns(['struct_flat'], 1, null, null),
    static fn() => $file->close(),
] as $call) {
    echo arrow_outcome($call), "\n";
}

echo "-- writer\n";
$filesystem = memory_filesystem();
$schema = Schema::with(FlatColumn::int32('id'), FlatColumn::string('name'));
$writer = static fn($stream, Compressions $compression = Compressions::SNAPPY, int $batchSize = 1_000): RustParquetFileWriter => new RustParquetFileWriter(
    $stream,
    SchemaConverter::toExtension($schema),
    $compression,
    OptionsConverter::toExtension(new Options()),
    $batchSize,
);
$values = static fn(string $uri): string => json_encode(iterator_to_array(Reader::php()->readStream($filesystem->readFrom(path($uri)))->values(), false));

$file = $writer($filesystem->writeTo(path('memory://doors.parquet')), Compressions::GZIP, 2);
var_dump($file instanceof ParquetFileWriter);
$file->writeRow(['id' => 1]);
$file->writeBatch([['id' => 2], 'key' => ['id' => 3, 'name' => 'c']]);
$file->writeBatch((static function () {
    yield ['id' => 4];
    yield 'key' => ['id' => 5];
})());
echo arrow_outcome(static fn() => $file->writeRow(['id' => 'six'])), "\n";
$file->writeBatch(new ArrayIterator([['id' => 6]]));
$file->writeColumns(['id' => [7, 8], 'unknown' => ['x', 'y']]);
echo arrow_outcome(static fn() => $file->writeBatch((static function () {
    yield ['id' => 9];

    throw new LogicException('the rows ran out');
})())), "\n";
$file->close();
echo $values('memory://doors.parquet'), "\n";

foreach ([
    static fn() => $file->writeRow(['id' => 1]),
    static fn() => $file->writeRows([['id' => 1]]),
    static fn() => $file->writeBatch([]),
    static fn() => $file->writeBatch(new ArrayIterator([['id' => 1]])),
    static fn() => $file->writeColumns(['id' => [1]]),
    static fn() => $file->close(),
] as $call) {
    echo arrow_outcome($call), "\n";
}

$failing = $writer(new FailingCloseDestinationStream($filesystem->writeTo(path('memory://failing.parquet'))));
echo arrow_outcome(static fn() => $failing->close()), "\n";
echo arrow_outcome(static fn() => $failing->close()), "\n";
// a refused flush still closes the stream; the flush's refusal is the one thrown
$inner = new class($filesystem->writeTo(path('memory://append.parquet'))) implements Flow\Filesystem\DestinationStream {
    public bool $closed = false;

    public function __construct(private Flow\Filesystem\DestinationStream $stream) {}

    public function append(string $data): self
    {
        $this->stream->append($data);

        return $this;
    }

    public function close(): void
    {
        $this->closed = true;
    }

    public function fromResource($resource): self
    {
        return $this;
    }

    public function isOpen(): bool
    {
        return !$this->closed;
    }

    public function path(): Flow\Filesystem\Path
    {
        return $this->stream->path();
    }
};
$appending = $writer(new FailingAppendDestinationStream($inner));
$appending->writeRow(['id' => 1, 'name' => 'a']);
echo arrow_outcome(static fn() => $appending->close()), "\n";
var_dump($inner->closed);
echo arrow_outcome(static fn() => $writer($filesystem->writeTo(path('memory://lzo.parquet')), Compressions::LZO)), "\n";
?>
--EXPECT--
-- reader
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
[30,30,10]
bool(true)
Flow\Parquet\Exception\RuntimeException: RustColumnsReader cannot rewind
Flow\Parquet\Exception\InvalidArgumentException: Parquet column "ts" holds INT96, which arrow reads only as a datetime (Option::INT_96_AS_DATETIME = true); read the file with \Flow\Parquet\Reader::php()
bool(true)
Flow\Parquet\Exception\RuntimeException: Parquet column "list.list.element" (a path into LIST/MAP) is not supported by the arrow Parquet reader; read the file with \Flow\Parquet\Reader::php()
Flow\Parquet\Exception\InvalidArgumentException: Given file is not valid Parquet file: Invalid Parquet file. Corrupt footer
Flow\Parquet\Exception\RuntimeException: Reader is not open
Flow\Parquet\Exception\RuntimeException: Reader is not open
Flow\Parquet\Exception\RuntimeException: Reader is not open
Flow\Parquet\Exception\RuntimeException: Reader is not open
Flow\Parquet\Exception\RuntimeException: Reader is not open
Flow\Parquet\Exception\RuntimeException: Reader is not open
Flow\Parquet\Exception\RuntimeException: Reader is not open
-- writer
bool(true)
Flow\Parquet\Exception\ValidationException: Column "id" row 1: expected int, got string
LogicException: the rows ran out
[{"id":1,"name":null},{"id":2,"name":null},{"id":3,"name":"c"},{"id":4,"name":null},{"id":5,"name":null},{"id":6,"name":null},{"id":7,"name":null},{"id":8,"name":null},{"id":9,"name":null}]
Flow\Parquet\Exception\RuntimeException: Writer is not open
Flow\Parquet\Exception\RuntimeException: Writer is not open
Flow\Parquet\Exception\RuntimeException: Writer is not open
Flow\Parquet\Exception\RuntimeException: Writer is not open
Flow\Parquet\Exception\RuntimeException: Writer is not open
Flow\Parquet\Exception\RuntimeException: Writer is not open
Flow\Filesystem\Exception\RuntimeException: Closing "memory://failing.parquet" failed
Flow\Parquet\Exception\RuntimeException: Writer is not open
Flow\Filesystem\Exception\RuntimeException: Appending to "memory://append.parquet" failed
bool(true)
Flow\Parquet\Exception\RuntimeException: LZO compression is not supported by the Arrow engine
