<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Rows;
use Generator;

use function array_keys;

final class SpyBucketsStorage implements BucketsStorage
{
    /**
     * @var array<string, true>
     */
    private array $liveBucketIds = [];

    public function __construct(
        private readonly BucketsStorage $inner,
    ) {}

    public function append(string $bucketId, Rows $rows): void
    {
        $this->liveBucketIds[$bucketId] = true;
        $this->inner->append($bucketId, $rows);
    }

    public function get(string $bucketId): Generator
    {
        yield from $this->inner->get($bucketId);
    }

    /**
     * @return list<string>
     */
    public function liveBucketIds(): array
    {
        return array_keys($this->liveBucketIds);
    }

    public function remove(string $bucketId): void
    {
        unset($this->liveBucketIds[$bucketId]);
        $this->inner->remove($bucketId);
    }

    public function set(string $bucketId, Rows $rows): void
    {
        $this->liveBucketIds[$bucketId] = true;
        $this->inner->set($bucketId, $rows);
    }
}
