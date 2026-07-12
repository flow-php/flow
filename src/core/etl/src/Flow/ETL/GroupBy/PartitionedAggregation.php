<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Sort\ExternalSort\BucketsCache;
use Generator;
use Throwable;

use function bin2hex;
use function count;
use function hexdec;
use function random_bytes;
use function substr;

final readonly class PartitionedAggregation
{
    /**
     * @param int<1, max> $partitions
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private BucketsCache $cache,
        private int $partitions,
        private int $batchSize,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->partitions < 1) {
            throw new InvalidArgumentException('Partitions count must be greater than 0, given: ' . $this->partitions);
        }

        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->batchSize);
        }
    }

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function aggregate(Generator $rows, FlowContext $context, GroupBy $groupBy): Generator
    {
        $runId = bin2hex(random_bytes(8));

        /** @var array<int, string> $buckets */
        $buckets = [];

        try {
            foreach ($rows as $batch) {
                /** @var array<int, list<Row>> $partitioned */
                $partitioned = [];

                foreach ($batch as $row) {
                    $partitioned[$this->partition((string) $groupBy->keyValues($row))][] = $row;
                }

                foreach ($partitioned as $partition => $partitionRows) {
                    $buckets[$partition] ??= 'group-by-' . $runId . '-partition-' . $partition;
                    $this->cache->append($buckets[$partition], new Rows(...$partitionRows));
                }
            }

            $aggregation = new BucketAggregation();

            foreach ($buckets as $bucketId) {
                yield from $aggregation->aggregate($this->bucketRows($bucketId), $context, $groupBy);
            }
        } finally {
            foreach ($buckets as $bucketId) {
                try {
                    $this->cache->remove($bucketId);
                } catch (Throwable) {
                }
            }
        }
    }

    /**
     * @return Generator<int, Rows>
     */
    private function bucketRows(string $bucketId): Generator
    {
        $buffer = [];

        foreach ($this->cache->get($bucketId) as $row) {
            $buffer[] = $row;

            if (count($buffer) >= $this->batchSize) {
                yield new Rows(...$buffer);
                $buffer = [];
            }
        }

        if ($buffer !== []) {
            yield new Rows(...$buffer);
        }
    }

    private function partition(string $key): int
    {
        return (int) hexdec(substr(NativePHPHash::xxh128($key), 0, 8)) % $this->partitions;
    }
}
