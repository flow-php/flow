<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing\Storage;

use Flow\ETL\Bucketing\ResidentBucketsStorage;
use Flow\ETL\Rows;
use Generator;

final class MemoryBuckets implements ResidentBucketsStorage
{
    /**
     * @var array<string, list<Rows>>
     */
    private array $buckets = [];

    public function append(string $bucketId, Rows $rows): void
    {
        if (!$rows->empty()) {
            $this->buckets[$bucketId][] = $rows;
        }
    }

    public function get(string $bucketId): Generator
    {
        foreach ($this->buckets[$bucketId] ?? [] as $batch) {
            yield $batch;
        }
    }

    public function remove(string $bucketId): void
    {
        unset($this->buckets[$bucketId]);
    }

    public function set(string $bucketId, Rows $rows): void
    {
        $this->buckets[$bucketId] = [];
        $this->append($bucketId, $rows);
    }
}
