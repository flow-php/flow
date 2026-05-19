<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\ExternalSort;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Generator;

interface BucketsCache
{
    /**
     * @return \Generator<Row>
     */
    public function get(string $bucketId): Generator;

    public function remove(string $bucketId): void;

    /**
     * @param iterable<Row>|Rows $rows
     */
    public function set(string $bucketId, iterable|Rows $rows): void;
}
