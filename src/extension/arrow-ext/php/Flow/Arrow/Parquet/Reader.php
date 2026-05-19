<?php

declare(strict_types=1);

namespace Flow\Arrow\Parquet;

use Flow\Arrow\RandomAccessFile;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('arrow')) {
    return;
}

/**
 * Stub for the Rust-defined Flow\Arrow\Parquet\Reader class.
 * When the arrow extension is loaded, this file is skipped — the extension provides the real implementation.
 * When the extension is NOT loaded, this stub provides type information for static analysis
 * and throws at runtime if someone tries to instantiate it.
 */
final class Reader
{
    /**
     * @param array<string, mixed> $options
     */
    public function __construct(RandomAccessFile $source, array $options = [])
    {
        throw new RuntimeException(
            'The arrow PHP extension is not loaded. Install ext-arrow to use Flow\Arrow\Parquet\Reader.',
        );
    }

    public function close(): void {}

    /**
     * @return array<string, mixed>
     */
    public function metadata(): array
    {
        return [];
    }

    /**
     * @param null|array<string> $columns
     *
     * @return null|array<string, array<mixed>>
     */
    public function readRowGroup(?array $columns = null): ?array
    {
        return null;
    }

    /**
     * @return array<array<string, mixed>>
     */
    public function schema(): array
    {
        return [];
    }
}
