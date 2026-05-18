<?php

declare(strict_types=1);

namespace Flow\Arrow\Parquet;

use Flow\Arrow\OutputStream;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('arrow')) {
    return;
}

/**
 * Stub for the Rust-defined Flow\Arrow\Parquet\Writer class.
 * When the arrow extension is loaded, this file is skipped — the extension provides the real implementation.
 * When the extension is NOT loaded, this stub provides type information for static analysis
 * and throws at runtime if someone tries to instantiate it.
 */
final class Writer
{
    /**
     * @param array<array<string, mixed>> $schema
     * @param array<string, mixed> $options
     */
    public function __construct(
        OutputStream $stream,
        array $schema,
        string $compression = 'SNAPPY',
        array $options = [],
    ) {
        throw new RuntimeException(
            'The arrow PHP extension is not loaded. Install ext-arrow to use Flow\Arrow\Parquet\Writer.',
        );
    }

    public function close(): void {}

    /**
     * @param array<string, array<mixed>> $batch
     */
    public function writeBatch(array $batch): void {}
}
