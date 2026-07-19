<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Generator;

interface BucketsStorage
{
    public function append(string $bucketId, Rows $rows): void;

    /**
     * @return Generator<Row>
     */
    public function get(string $bucketId): Generator;

    public function remove(string $bucketId): void;

    public function set(string $bucketId, Rows $rows): void;
}
