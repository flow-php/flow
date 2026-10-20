<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Arrow\Parquet\RustColumnsReader;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\Reader\ReadPlan;
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
     * Closes the stream; any later call throws \Flow\Parquet\Exception\RuntimeException('Reader is not open'). A
     * footer taken before stays readable.
     */
    public function close(): void
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * Created on the first call.
     */
    public function footer(): RustParquetFooter
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * @param list<string> $columns resolved names, never empty
     * @param int $batchSize upper bound of rows per chunk, refused below 1
     */
    public function readColumns(array $columns, int $batchSize, ReadPlan $plan): RustColumnsReader
    {
        throw new RuntimeException('arrow extension is not loaded');
    }
}
