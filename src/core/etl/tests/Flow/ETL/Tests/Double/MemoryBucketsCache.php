<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Sort\ExternalSort\BucketsCache;
use Generator;

final class MemoryBucketsCache implements BucketsCache
{
    /**
     * @var array<string, int>
     */
    public array $appendCalls = [];

    /**
     * @var array<string, list<Row>>
     */
    private array $buckets = [];

    public function append(string $bucketId, iterable|Rows $rows): void
    {
        $this->appendCalls[$bucketId] = ($this->appendCalls[$bucketId] ?? 0) + 1;

        foreach ($rows as $row) {
            $this->buckets[$bucketId][] = $row;
        }
    }

    public function get(string $bucketId): Generator
    {
        foreach ($this->buckets[$bucketId] ?? [] as $row) {
            yield $row;
        }
    }

    public function remove(string $bucketId): void
    {
        unset($this->buckets[$bucketId]);
    }

    public function set(string $bucketId, iterable|Rows $rows): void
    {
        $this->buckets[$bucketId] = [];
        $this->append($bucketId, $rows);
    }
}
