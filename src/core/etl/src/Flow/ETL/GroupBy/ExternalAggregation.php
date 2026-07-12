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

final readonly class ExternalAggregation
{
    /**
     * @param int<1, max> $bucketsCount
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private BucketsCache $cache,
        private int $bucketsCount,
        private int $batchSize,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->bucketsCount < 1) {
            throw new InvalidArgumentException('Buckets count must be greater than 0, given: ' . $this->bucketsCount);
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
                /** @var array<int, list<Row>> $bucketed */
                $bucketed = [];

                foreach ($batch as $row) {
                    $bucketed[$this->bucket((string) $groupBy->keyValues($row))][] = $row;
                }

                foreach ($bucketed as $bucket => $bucketRows) {
                    $buckets[$bucket] ??= 'group-by-' . $runId . '-bucket-' . $bucket;
                    $this->cache->append($buckets[$bucket], new Rows(...$bucketRows));
                }
            }

            $aggregation = new BucketAggregation();

            foreach ($buckets as $bucketId) {
                foreach ($aggregation->aggregate($this->bucketRows($bucketId), $context, $groupBy) as $resultBatch) {
                    yield $resultBatch;
                }
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

    private function bucket(string $key): int
    {
        return (int) hexdec(substr(NativePHPHash::xxh128($key), 0, 8)) % $this->bucketsCount;
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
}
