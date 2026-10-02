<?php

declare(strict_types=1);

namespace Flow\Arrow;

use RuntimeException;

use function extension_loaded;

if (extension_loaded('arrow')) {
    return;
}

/**
 * One Arrow C Data struct schema; built by Flow\Arrow\Parquet\BatchReader::schema(), never by userland.
 */
final class ArrowSchema
{
    public function __construct()
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * FFI_ArrowSchema* of the struct type; its consumer reads it by reference.
     */
    public function arrowSchemaAddress(): int
    {
        throw new RuntimeException('arrow extension is not loaded');
    }
}
