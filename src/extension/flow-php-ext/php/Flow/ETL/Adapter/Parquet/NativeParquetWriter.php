<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\Arrow\Parquet\RowsWriter;
use Flow\ETL\Rows;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class NativeParquetWriter
{
    /**
     * @param RowsWriter $writer arrow-ext's writer, which owns the stream
     */
    public function __construct(RowsWriter $writer)
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * The writer's buffered rows, the footer, then the stream closed.
     */
    public function close(): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * Every column of the rows as one Arrow C Data batch; a writer column the rows lack is written as nulls.
     */
    public function write(Rows $rows): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
