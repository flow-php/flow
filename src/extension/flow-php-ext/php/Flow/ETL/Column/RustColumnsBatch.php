<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

/**
 * One Arrow C Data struct array and its schema; built by Flow\ETL\Adapter\Parquet\RustParquetOpenSink::write(), never
 * by userland.
 */
final class RustColumnsBatch
{
    public function __construct()
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * FFI_ArrowArray* of the struct array; its consumer moves it out.
     */
    public function arrowArrayAddress(): int
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * FFI_ArrowSchema* of the struct type; its consumer reads it by reference.
     */
    public function arrowSchemaAddress(): int
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function count(): int
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
