--TEST--
RustParquetFileReader decodes the footer to the FileMetaData and schema the PHP thrift reader builds, from one footer read
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Filesystem\Stream\{MemorySourceStream, NativeLocalSourceStream};
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType, PhysicalType};
use Flow\Parquet\ParquetFile\Schema\LogicalType\{Time, Timestamp};
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Writer;
use Flow\Parquet\Engine\RustParquetFileReader;
use Flow\Parquet\Tests\Double\ReadCountingSourceStream;
use Flow\Parquet\Thrift\{CompactProtocol, MemoryBuffer};
use Flow\Parquet\ThriftModel\FileMetaData;

use function Flow\Filesystem\DSL\path_real;

$fixtures = realpath(__DIR__ . '/../../../../lib/parquet/tests/Flow/Parquet/Tests/Integration/IO/Fixtures');

foreach ([...glob($fixtures . '/*.parquet'), ...glob($fixtures . '/EdgeCases/*.parquet')] as $path) {
    $bytes = file_get_contents($path);
    $length = unpack('V', substr($bytes, -8, 4))[1];
    $php = new FileMetaData();
    $php->read(new CompactProtocol(new MemoryBuffer(substr($bytes, -8 - $length, $length))));

    $stream = new ReadCountingSourceStream(NativeLocalSourceStream::open(path_real($path)));
    $file = new RustParquetFileReader($stream);
    $reads = $stream->reads;

    printf(
        "%s: thrift %s, schema %s, footer reads %d\n",
        substr($path, strlen($fixtures) + 1),
        serialize($file->thrift()) === serialize($php) ? 'identical' : 'DIFFER',
        serialize($file->schema()) === serialize(Schema::fromThrift($php->schema)) ? 'identical' : 'DIFFER',
        $reads,
    );
    $file->close();
}

// the temporal units and the UTC flag of every TIMESTAMP / TIME unit
$units = MemoryParquetFile::written(Writer::php(), Schema::with(
    FlatColumn::dateTime('ts'),
    new FlatColumn('ts_ms', PhysicalType::INT64, logicalType: new LogicalType(LogicalType::TIMESTAMP, timestamp: new Timestamp(false, TimeUnit::MILLISECONDS))),
    new FlatColumn('ts_ns', PhysicalType::INT64, logicalType: new LogicalType(LogicalType::TIMESTAMP, timestamp: new Timestamp(true, TimeUnit::NANOSECONDS))),
    new FlatColumn('t_ms', PhysicalType::INT32, logicalType: new LogicalType(LogicalType::TIME, time: new Time(false, TimeUnit::MILLISECONDS))),
    new FlatColumn('t_ns', PhysicalType::INT64, logicalType: new LogicalType(LogicalType::TIME, time: new Time(false, TimeUnit::NANOSECONDS))),
), [['ts' => null, 'ts_ms' => null, 'ts_ns' => null, 't_ms' => null, 't_ns' => null]]);
$length = unpack('V', substr($units, -8, 4))[1];
$php = new FileMetaData();
$php->read(new CompactProtocol(new MemoryBuffer(substr($units, -8 - $length, $length))));
$file = new RustParquetFileReader(new MemorySourceStream($units));

printf(
    "temporal units: thrift %s, schema %s\n",
    serialize($file->thrift()) === serialize($php) ? 'identical' : 'DIFFER',
    serialize($file->schema()) === serialize(Schema::fromThrift($php->schema)) ? 'identical' : 'DIFFER',
);
$file->close();
?>
--EXPECT--
columns.required.parquet: thrift identical, schema identical, footer reads 1
decimals_fixed_len.parquet: thrift identical, schema identical, footer reads 1
delta_binary_acked_encoded_integers.parquet: thrift identical, schema identical, footer reads 1
lists.parquet: thrift identical, schema identical, footer reads 1
logical_types.parquet: thrift identical, schema identical, footer reads 1
maps.parquet: thrift identical, schema identical, footer reads 1
multiple_pages.parquet: thrift identical, schema identical, footer reads 1
pagination_row_group_1kb_5k_rows.snappy.parquet: thrift identical, schema identical, footer reads 2
primitives.parquet: thrift identical, schema identical, footer reads 1
structs.parquet: thrift identical, schema identical, footer reads 1
EdgeCases/datapage_v2.snappy.parquet: thrift identical, schema identical, footer reads 1
EdgeCases/int96.parquet: thrift identical, schema identical, footer reads 1
EdgeCases/interval.parquet: thrift identical, schema identical, footer reads 1
EdgeCases/map_keys.parquet: thrift identical, schema identical, footer reads 1
EdgeCases/nonnullable.impala.parquet: thrift identical, schema identical, footer reads 1
EdgeCases/null_list.parquet: thrift identical, schema identical, footer reads 1
EdgeCases/uint64_overflow.parquet: thrift identical, schema identical, footer reads 1
EdgeCases/unsigned.parquet: thrift identical, schema identical, footer reads 1
temporal units: thrift identical, schema identical
