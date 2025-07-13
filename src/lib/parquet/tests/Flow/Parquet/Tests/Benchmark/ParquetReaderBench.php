<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Benchmark;

use Flow\Filesystem\{Path, Stream\NativeLocalSourceStream};
use Flow\Parquet\{ByteOrder, Options, ParquetFile, ParquetFile\Data\DataConverter};
use PhpBench\Attributes\Groups;

#[Groups(['parquet-library'])]
final readonly class ParquetReaderBench
{
    private ParquetFile $parquetFile;

    public function __construct()
    {
        $stream = NativeLocalSourceStream::open(new Path(__DIR__ . '/Fixtures/orders_1k.parquet'));

        $this->parquetFile = new ParquetFile(
            $stream,
            ByteOrder::LITTLE_ENDIAN,
            DataConverter::initialize(new Options()),
            new Options()
        );
    }

    public function bench_page_headers() : void
    {
        foreach ($this->parquetFile->pageHeaders() as $pageHeader) {
            // Just iterate through page headers
        }
    }

    public function bench_read_metadata() : void
    {
        $this->parquetFile->metadata();
    }

    public function bench_read_schema() : void
    {
        $this->parquetFile->schema();
    }

    public function bench_read_values_all_columns() : void
    {
        foreach ($this->parquetFile->values() as $row) {
            // Just iterate through all values
        }
    }

    public function bench_read_values_single_column() : void
    {
        $columns = $this->parquetFile->schema()->columns();
        $firstColumn = $columns[0]->name();

        foreach ($this->parquetFile->values([$firstColumn]) as $row) {
            // Just iterate through single column values
        }
    }

    public function bench_read_values_with_limit() : void
    {
        foreach ($this->parquetFile->values([], 100) as $row) {
            // Just iterate through limited values
        }
    }
}
