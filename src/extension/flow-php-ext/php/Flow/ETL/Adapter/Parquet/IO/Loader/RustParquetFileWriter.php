<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\IO\Loader;

use Flow\ETL\IO\Loader\File\FileWriter;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\Parquet\Engine\RustParquetFileWriter as ArrowRustParquetFileWriter;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class RustParquetFileWriter implements FileWriter
{
    /**
     * @param ArrowRustParquetFileWriter $writer arrow-ext's writer, which owns the stream
     * @param Backend $backend the backend a cast column is built with
     */
    public function __construct(
        ArrowRustParquetFileWriter $writer,
        ParquetSchemaConformance $conformance,
        Backend $backend,
    ) {
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
     * Every column of the conformed rows as one Arrow C Data batch; a writer column the rows lack is written as nulls.
     */
    public function write(Rows $rows): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
