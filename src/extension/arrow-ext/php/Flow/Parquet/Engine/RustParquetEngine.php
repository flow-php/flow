<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('arrow')) {
    return;
}

final class RustParquetEngine implements ParquetEngine
{
    public function __construct(?Options $options = null)
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    public function openForRead(SourceStream $stream): RustParquetFileReader
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * @throws \Flow\Parquet\Exception\RuntimeException for Compressions::LZO
     */
    public function openForWrite(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
    ): RustParquetFileWriter {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * @param iterable<array<array-key, mixed>> $rows
     */
    public function writeRows(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
        iterable $rows,
    ): void {
        throw new RuntimeException('arrow extension is not loaded');
    }
}
