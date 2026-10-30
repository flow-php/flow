<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

/**
 * Fails on remove(), which is what Buckets::clear() calls. FilesystemBuckets::remove() does real I/O
 * (closeWriter() then filesystem->rm()), so a throwing clear is reachable in production.
 */
final class ThrowingRemoveRowsStorage implements RowsStorage
{
    public function __construct(
        private readonly RowsStorage $inner,
    ) {}

    public function append(string $key, Rows $rows): void
    {
        $this->inner->append($key, $rows);
    }

    public function close(string $key): void
    {
        $this->inner->close($key);
    }

    public function get(string $key): Generator
    {
        yield from $this->inner->get($key);
    }

    public function has(string $key): bool
    {
        return $this->inner->has($key);
    }

    public function remove(string $key): void
    {
        throw new RuntimeException('spill clear failed');
    }

    public function rows(string $key): Cardinality
    {
        return $this->inner->rows($key);
    }

    public function schema(string $key): ?Schema
    {
        return $this->inner->schema($key);
    }
}
