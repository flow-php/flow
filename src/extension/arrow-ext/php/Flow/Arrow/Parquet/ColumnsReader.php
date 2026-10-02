<?php

declare(strict_types=1);

namespace Flow\Arrow\Parquet;

use RuntimeException;

use function extension_loaded;

if (extension_loaded('arrow')) {
    return;
}

final class ColumnsReader
{
    /**
     * @param list<string> $columns root names or struct paths
     * @param int<1, max> $batchSize
     */
    public function __construct(ParquetFile $file, array $columns, int $batchSize, ?int $offset, ?int $limit)
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    public function close(): void
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * At most $batchSize rows, keyed by $columns, null after the last.
     *
     * @return null|array<string, non-empty-list<mixed>>
     */
    public function next(): ?array
    {
        throw new RuntimeException('arrow extension is not loaded');
    }
}
