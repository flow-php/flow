<?php

declare(strict_types=1);

namespace Flow\ETL\Join\HashJoin;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Sort\ExternalSort\BucketsCache;

final class BucketSpiller
{
    /**
     * @var array<int, string>
     */
    private array $bucketIds = [];

    /**
     * @var array<int, list<Row>>
     */
    private array $pending = [];

    private int $pendingCount = 0;

    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private readonly BucketsCache $cache,
        private readonly string $bucketIdPrefix,
        private readonly int $batchSize,
    ) {}

    public function add(int $bucket, Row $row): void
    {
        $this->pending[$bucket][] = $row;
        $this->pendingCount++;

        if ($this->pendingCount >= $this->batchSize) {
            $this->flush();
        }
    }

    /**
     * @return array<int, string>
     */
    public function bucketIds(): array
    {
        return $this->bucketIds;
    }

    public function flush(): void
    {
        foreach ($this->pending as $bucket => $rows) {
            if ($rows !== []) {
                $this->cache->append($this->bucketId($bucket), new Rows(...$rows));
            }
        }

        $this->pending = [];
        $this->pendingCount = 0;
    }

    private function bucketId(int $bucket): string
    {
        return $this->bucketIds[$bucket] ??= $this->bucketIdPrefix . $bucket;
    }
}
