<?php

declare(strict_types=1);

namespace Flow\Arrow\Parquet;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\ThriftModel\FileMetaData;
use Flow\Parquet\ThriftModel\SchemaElement;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('arrow')) {
    return;
}

final class ParquetFile
{
    /**
     * Reads and decodes the footer; owns the stream from here on.
     */
    public function __construct(SourceStream $stream)
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * Closes the stream; any later call throws \Flow\Parquet\Exception\RuntimeException('Parquet file is closed').
     */
    public function close(): void
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    public function rowsNumber(): int
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * @return list<SchemaElement>
     */
    public function schema(): array
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
