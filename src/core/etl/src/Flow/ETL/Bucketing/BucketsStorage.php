<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Rows;
use Generator;

interface BucketsStorage
{
    public function append(string $bucketId, Rows $rows): void;

    /**
     * Yields the bucket back in batches - batch shape is storage-defined, consumers must not
     * assume any particular batch size.
     *
     * @return Generator<Rows>
     */
    public function get(string $bucketId): Generator;

    public function remove(string $bucketId): void;

    public function set(string $bucketId, Rows $rows): void;
}
