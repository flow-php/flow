<?php

declare(strict_types=1);

namespace Flow\ETL\Join;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Join\HashJoin\BucketSpiller;
use Flow\ETL\Join\HashJoin\Joiner;
use Flow\ETL\Join\HashJoin\NullRowBuilder;
use Flow\ETL\Rows;
use Flow\ETL\Sort\ExternalSort\BucketsCache;
use Flow\ETL\Sort\ExternalSort\ResidentBucketsCache;
use Generator;
use Throwable;

use function array_intersect_key;
use function array_keys;
use function array_values;
use function bin2hex;
use function count;
use function hexdec;
use function random_bytes;
use function substr;

/**
 * Hash join with storage-driven execution. The right side is always partitioned through
 * the BucketsCache. With a resident (in-memory) cache the left side is streamed through
 * a single hash table - left row order is preserved and memory usage is bounded by the
 * right side. With a non-resident (on-disk) cache both sides are partitioned by join key
 * hash and joined one pair of buckets at a time (grace hash join) - memory usage is
 * bounded by the largest bucket and left row order is not preserved.
 */
final readonly class BucketedHashJoin
{
    /**
     * @param int<1, max> $bucketsCount
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private DataFrame $right,
        private Expression $expression,
        private Join $type,
        private BucketsCache $cache,
        private int $bucketsCount = 64,
        private int $batchSize = 1000,
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
     * @param Generator<Rows> $left
     *
     * @return Generator<Rows>
     */
    public function joinGenerator(Generator $left, FlowContext $context): Generator
    {
        $joiner = new Joiner($this->expression, $this->type, $context->entryFactory(), $this->batchSize);
        $resident = $this->cache instanceof ResidentBucketsCache;
        $runId = bin2hex(random_bytes(8));

        $rightSpiller = new BucketSpiller($this->cache, 'join-' . $runId . '-right-bucket-', $this->batchSize);
        $leftSpiller = new BucketSpiller($this->cache, 'join-' . $runId . '-left-bucket-', $this->batchSize);

        try {
            $nullRightBuilder = $this->type === Join::left ? new NullRowBuilder($context->entryFactory()) : null;

            foreach ($this->right->get() as $batch) {
                foreach ($batch as $row) {
                    $nullRightBuilder?->collect($row);
                    // a resident cache is consumed as one bucket, hashing rows into buckets would be wasted work
                    $rightSpiller->add($resident ? 0 : $this->bucket($joiner->keys()->rightHash($row)), $row);
                }
            }

            $rightSpiller->flush();
            $rightBuckets = $rightSpiller->bucketIds();
            $nullRightRow = $nullRightBuilder?->row();

            if ($resident) {
                yield from $joiner->join($left, $this->bucketRows($rightBuckets[0] ?? null), null, $nullRightRow);

                return;
            }

            $nullLeftBuilder = $this->type === Join::right ? new NullRowBuilder($context->entryFactory()) : null;

            foreach ($left as $batch) {
                foreach ($batch as $row) {
                    $nullLeftBuilder?->collect($row);
                    $leftSpiller->add($this->bucket($joiner->keys()->leftHash($row)), $row);
                }
            }

            $leftSpiller->flush();
            $leftBuckets = $leftSpiller->bucketIds();

            $buckets = match ($this->type) {
                Join::inner => array_keys(array_intersect_key($leftBuckets, $rightBuckets)),
                Join::left, Join::left_anti => array_keys($leftBuckets),
                Join::right => array_keys($rightBuckets),
            };

            $nullLeftRow = $nullLeftBuilder?->row();

            foreach ($buckets as $bucket) {
                $joinedBatches = $joiner->join(
                    $this->bucketRows($leftBuckets[$bucket] ?? null),
                    $this->bucketRows($rightBuckets[$bucket] ?? null),
                    $nullLeftRow,
                    $nullRightRow,
                );

                // re-key batches, yield from would restart keys at 0 for every bucket
                foreach ($joinedBatches as $joinedBatch) {
                    yield $joinedBatch;
                }
            }
        } finally {
            foreach ([
                ...array_values($leftSpiller->bucketIds()),
                ...array_values($rightSpiller->bucketIds()),
            ] as $bucketId) {
                try {
                    $this->cache->remove($bucketId);
                } catch (Throwable) {
                }
            }
        }
    }

    private function bucket(string $hash): int
    {
        // JoinKeys hashes are xxh128 hex strings, reuse their first 8 chars instead of hashing again
        return (int) hexdec(substr($hash, 0, 8)) % $this->bucketsCount;
    }

    /**
     * @return Generator<Rows>
     */
    private function bucketRows(?string $bucketId): Generator
    {
        if ($bucketId === null) {
            return;
        }

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
