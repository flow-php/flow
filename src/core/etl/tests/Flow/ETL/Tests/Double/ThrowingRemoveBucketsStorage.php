<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Rows;
use Generator;

/**
 * Fails on remove(), which is what Buckets::clear() calls. FilesystemBuckets::remove() does real I/O
 * (closeWriter() then filesystem->rm()), so a throwing clear is reachable in production.
 */
final class ThrowingRemoveBucketsStorage implements BucketsStorage
{
    public function __construct(
        private readonly BucketsStorage $inner,
    ) {}

    public function append(string $bucketId, Rows $rows): void
    {
        $this->inner->append($bucketId, $rows);
    }

    public function get(string $bucketId): Generator
    {
        yield from $this->inner->get($bucketId);
    }

    public function remove(string $bucketId): void
    {
        throw new RuntimeException('spill clear failed');
    }

    public function set(string $bucketId, Rows $rows): void
    {
        $this->inner->set($bucketId, $rows);
    }
}
