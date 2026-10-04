<?php

declare(strict_types=1);

namespace Flow\Arrow\Parquet;

use Flow\Parquet\Engine\RustParquetFileReader;
use Iterator;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('arrow')) {
    return;
}

/**
 * Streams once: rewind() after next() throws \Flow\Parquet\Exception\RuntimeException('RustColumnsReader cannot rewind').
 *
 * @implements Iterator<int, array<string, non-empty-list<mixed>>>
 */
final class RustColumnsReader implements Iterator
{
    /**
     * @param list<string> $columns root names or struct paths
     * @param int<1, max> $batchSize
     */
    public function __construct(RustParquetFileReader $file, array $columns, int $batchSize, ?int $offset, ?int $limit)
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * At most $batchSize rows, keyed by $columns.
     *
     * @return null|array<string, non-empty-list<mixed>>
     */
    public function current(): ?array
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    public function key(): ?int
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    public function next(): void
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    public function rewind(): void
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    public function valid(): bool
    {
        throw new RuntimeException('arrow extension is not loaded');
    }
}
