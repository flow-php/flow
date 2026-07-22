<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Bucketing\Bucket;
use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\HashBucketing;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Bucketing\ResidentBucketsStorage;
use Flow\ETL\Bucketing\SingleBucketHasher;
use Flow\ETL\DataFrame;
use Flow\ETL\Exception\DuplicatedEntriesException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\JoinException;
use Flow\ETL\FlowContext;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\HashJoin\Joiner;
use Flow\ETL\Join\HashJoin\NullRowBuilder;
use Flow\ETL\Join\Join;
use Flow\ETL\Processor;
use Flow\ETL\RandomValueGenerator;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Generator;

use function array_intersect_key;
use function array_keys;

/**
 * @internal
 */
final readonly class HashJoinProcessor implements Processor
{
    /**
     * @param int<1, max> $bucketsCount
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private DataFrame $right,
        private Expression $expression,
        private Join $type,
        private Buckets $leftBuckets,
        private Buckets $rightBuckets,
        private RandomValueGenerator $random,
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

    public function process(Generator $rows, FlowContext $context): Generator
    {
        $joiner = new Joiner($this->expression, $this->type, $context->entryFactory(), $this->batchSize);
        $equalityKeys = $joiner->keys();
        $resident = $this->rightBuckets->storage() instanceof ResidentBucketsStorage;

        try {
            $nullRightBuilder = $this->type === Join::left ? new NullRowBuilder($context->entryFactory()) : null;

            $rightRows = $this->tap(
                $this->right->get(),
                $nullRightBuilder,
                // right rows with a null join key can never match, they only surface in right join output
                $equalityKeys !== null && $this->type !== Join::right ? $equalityKeys->rightRefs() : null,
            );

            $this->bucketize(
                $rightRows,
                $this->rightBuckets,
                $equalityKeys !== null && !$resident ? $equalityKeys->rightRefs() : null,
                'join-right',
            );

            $nullRightRow = $nullRightBuilder?->row();

            // in the resident path the Joiner collects the null-left row itself while streaming
            $nullLeftBuilder = !$resident && $this->type === Join::right
                ? new NullRowBuilder($context->entryFactory())
                : null;

            $leftRows = $this->tap(
                $rows,
                $nullLeftBuilder,
                // left rows with a null join key can never match, they only surface in left/left_anti output
                $equalityKeys !== null && $this->type !== Join::left && $this->type !== Join::left_anti
                    ? $equalityKeys->leftRefs()
                    : null,
            );

            if ($resident) {
                $rightBucket = $this->rightBuckets->all()[0] ?? null;

                yield from $joiner->join(
                    $leftRows,
                    $rightBucket === null ? self::noRows() : $this->rightBuckets->rows($rightBucket->id),
                    null,
                    $nullRightRow,
                );

                return;
            }

            $this->bucketize($leftRows, $this->leftBuckets, $equalityKeys?->leftRefs(), 'join-left');

            $nullLeftRow = $nullLeftBuilder?->row();

            foreach ($this->bucketPairs() as [$leftBucket, $rightBucket]) {
                $joinedBatches = $joiner->join(
                    $leftBucket === null ? self::noRows() : $this->leftBuckets->rows($leftBucket->id),
                    $rightBucket === null ? self::noRows() : $this->rightBuckets->rows($rightBucket->id),
                    $nullLeftRow,
                    $nullRightRow,
                    buildLeft: ($leftBucket->totalRows ?? 0) < ($rightBucket->totalRows ?? 0),
                );

                // re-key batches, yield from would restart keys at 0 for every bucket pair
                foreach ($joinedBatches as $joinedBatch) {
                    yield $joinedBatch;
                }
            }
        } catch (DuplicatedEntriesException $e) {
            throw new JoinException($e->getMessage(), (int) $e->getCode(), $e);
        } finally {
            $this->leftBuckets->clear();
            $this->rightBuckets->clear();
        }
    }

    /**
     * @param Generator<Rows> $rows
     * @param null|list<Reference> $refs - null hashes every row into a single bucket (non-equality join / resident side)
     */
    private function bucketize(Generator $rows, Buckets $buckets, ?array $refs, string $namespace): void
    {
        $strategy = $refs !== null
            ? new HashBucketing($refs, $this->bucketsCount, new NativeHasher(), $this->random, $namespace)
            : new HashBucketing([], 1, new SingleBucketHasher(), $this->random, $namespace);

        foreach ($strategy->bucketize($rows, $buckets->storage()) as $bucket) {
            $buckets->add($bucket);
        }
    }

    /**
     * Pairs buckets by partition index, processed in discovery order. A bucket that received no
     * rows was never created - a missing side means an empty side.
     *
     * @return Generator<array{?Bucket, ?Bucket}>
     */
    private function bucketPairs(): Generator
    {
        /** @var array<int, Bucket> $leftBuckets */
        $leftBuckets = [];

        foreach ($this->leftBuckets->all() as $bucket) {
            $leftBuckets[$bucket->index] = $bucket;
        }

        /** @var array<int, Bucket> $rightBuckets */
        $rightBuckets = [];

        foreach ($this->rightBuckets->all() as $bucket) {
            $rightBuckets[$bucket->index] = $bucket;
        }

        $indexes = match ($this->type) {
            Join::inner => array_keys(array_intersect_key($leftBuckets, $rightBuckets)),
            Join::left, Join::left_anti => array_keys($leftBuckets),
            Join::right => array_keys($rightBuckets),
        };

        foreach ($indexes as $index) {
            yield [$leftBuckets[$index] ?? null, $rightBuckets[$index] ?? null];
        }
    }

    /**
     * @return Generator<Rows>
     */
    private static function noRows(): Generator
    {
        yield from [];
    }

    /**
     * Feeds every row to the null-row builder, then drops rows whose join key contains null - those
     * can never match and their unmatched padding never reaches the output for the given join type.
     *
     * @param Generator<Rows> $rows
     * @param null|array<Reference> $dropNullKeyRefs
     *
     * @return Generator<Rows>
     */
    private function tap(Generator $rows, ?NullRowBuilder $nullRowBuilder, ?array $dropNullKeyRefs): Generator
    {
        foreach ($rows as $batch) {
            if ($nullRowBuilder !== null) {
                foreach ($batch as $row) {
                    $nullRowBuilder->collect($row);
                }
            }

            if ($dropNullKeyRefs !== null) {
                $batch = $batch->filter(static function (Row $row) use ($dropNullKeyRefs): bool {
                    foreach ($dropNullKeyRefs as $ref) {
                        if ($row->valueOf($ref) === null) {
                            return false;
                        }
                    }

                    return true;
                });

                if ($batch->empty()) {
                    continue;
                }
            }

            yield $batch;
        }
    }
}
