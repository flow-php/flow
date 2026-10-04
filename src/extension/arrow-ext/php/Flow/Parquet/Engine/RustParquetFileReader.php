<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Arrow\Parquet\RustColumnsReader;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\ThriftModel\FileMetaData;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('arrow')) {
    return;
}

final class RustParquetFileReader implements ParquetFileReader
{
    /**
     * Reads and decodes the footer; owns the stream from here on. $int96AsDatetime false refuses to read an INT96
     * column, which arrow reads only as a datetime.
     */
    public function __construct(SourceStream $stream, bool $int96AsDatetime = true)
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * Closes the stream; any later call throws \Flow\Parquet\Exception\RuntimeException('Reader is not open').
     */
    public function close(): void
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    public function metadata(): Metadata
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * @param list<string> $columns resolved names, never empty
     * @param int<1, max> $batchSize upper bound of rows per chunk
     */
    public function readColumns(array $columns, int $batchSize, ?int $limit, ?int $offset): RustColumnsReader
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    public function rowsNumber(): int
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    public function schema(): Schema
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * The whole footer, decoded on the first call.
     */
    public function thrift(): FileMetaData
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * Σ row group total_byte_size (uncompressed).
     */
    public function totalByteSize(): int
    {
        throw new RuntimeException('arrow extension is not loaded');
    }
}
