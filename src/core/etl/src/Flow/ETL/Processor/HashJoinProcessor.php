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
use Flow\ETL\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\HashJoin\Joiner;
use Flow\ETL\Join\HashJoin\JoinSide;
use Flow\ETL\Join\HashJoin\NullRowBuilder;
use Flow\ETL\Join\Join;
use Flow\ETL\Join\JoinShape;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Processor;
use Flow\ETL\RandomValueGenerator;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function array_intersect_key;
use function array_keys;

/**
 * @internal
 */
final class HashJoinProcessor implements Processor
{
    /**
     * The schemas this step declares. Only bind() sets them; the unbound path discovers each side
     * from the rows that flow.
     */
    private ?Schema $left = null;

    private ?Schema $declaredRight = null;

    /**
     * @param int<1, max> $bucketsCount
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private readonly DataFrame $right,
        private readonly Expression $expression,
        private readonly Join $type,
        private readonly Buckets $leftBuckets,
        private readonly Buckets $rightBuckets,
        private readonly RandomValueGenerator $random,
        private readonly int $bucketsCount = 64,
        private readonly int $batchSize = 1000,
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

    public function bind(Schema $input): BoundStep
    {
        $right = $this->right->schema();

        $bound = new self(
            $this->right,
            $this->expression,
            $this->type,
            $this->leftBuckets,
            $this->rightBuckets,
            $this->random,
            $this->bucketsCount,
            $this->batchSize,
        );
        $bound->left = $input;
        $bound->declaredRight = $right;

        return new BoundStep($bound, JoinShape::of($this->expression, $this->type)->schema()->of(
            $this->type,
            $input,
            $right,
        ));
    }

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function process(Generator $rows, FlowContext $context): Generator
    {
        $leftSchema = $this->left;
        $rightSchema = $this->declaredRight;

        if ($rightSchema === null) {
            try {
                $rightSchema = $this->right->schema();
            } catch (SchemaNotDerivableException) {
                // an undescribable right side is still joinable, its shape just has to be
                // discovered from the rows that flow
                $rightSchema = null;
            }
        }

        $joiner = new Joiner($this->expression, $this->type, $this->batchSize);
        $equalityKeys = $joiner->keys();
        $resident = $this->rightBuckets->storage() instanceof ResidentBucketsStorage;

        try {
            $rightRows = $this->tap(
                $this->right->get(),
                $rightSchema,
                // right rows with a null join key can never match, they only surface in right join output
                $equalityKeys !== null && $this->type !== Join::right ? $equalityKeys->rightRefs() : null,
            );

            $this->bucketize(
                $rightRows,
                $this->rightBuckets,
                $equalityKeys !== null && !$resident ? $equalityKeys->rightRefs() : null,
                'join-right',
            );

            $nullRightRow = $this->type === Join::left
                ? (new NullRowBuilder($rightSchema ?? new Schema()))->row()
                : null;

            $leftRows = $this->tap(
                $rows,
                $leftSchema,
                // left rows with a null join key can never match, they only surface in left/left_anti output
                $equalityKeys !== null && $this->type !== Join::left && $this->type !== Join::left_anti
                    ? $equalityKeys->leftRefs()
                    : null,
            );

            if ($resident) {
                $rightBucket = $this->rightBuckets->all()[0] ?? null;

                yield from $joiner->join(
                    JoinSide::of($leftRows, null, $leftSchema),
                    JoinSide::of(
                        $rightBucket === null ? self::noRows() : $this->rightBuckets->rows($rightBucket->id),
                        $nullRightRow,
                        $rightSchema,
                    ),
                );

                return;
            }

            $this->bucketize($leftRows, $this->leftBuckets, $equalityKeys?->leftRefs(), 'join-left');

            // only the bucketized path builds it here, the resident path above derives the null-left
            // row inside the Joiner while streaming
            $nullLeftRow = $this->type === Join::right
                ? (new NullRowBuilder($leftSchema ?? new Schema()))->row()
                : null;

            foreach ($this->bucketPairs() as [$leftBucket, $rightBucket]) {
                // a bucket pair is a slice of each side; the output shape is the whole side's
                $joinedBatches = $joiner->join(
                    JoinSide::of(
                        $leftBucket === null ? self::noRows() : $this->leftBuckets->rows($leftBucket->id),
                        $nullLeftRow,
                        $leftSchema,
                    ),
                    JoinSide::of(
                        $rightBucket === null ? self::noRows() : $this->rightBuckets->rows($rightBucket->id),
                        $nullRightRow,
                        $rightSchema,
                    ),
                    buildLeft: ($leftBucket->totalRows ?? 0) < ($rightBucket->totalRows ?? 0),
                );

                // re-key batches, yield from would restart keys at 0 for every bucket pair
                foreach ($joinedBatches as $joinedBatch) {
                    yield $joinedBatch;
                }
            }
        } catch (DuplicatedEntriesException|SchemaDefinitionNotUniqueException $e) {
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
     * @param null|Schema $schema - out parameter, the schema of the first batch flowing through
     * @param null|array<Reference> $dropNullKeyRefs
     *
     * @return Generator<Rows>
     */
    private function tap(Generator $rows, ?Schema &$schema, ?array $dropNullKeyRefs): Generator
    {
        foreach ($rows as $batch) {
            $schema ??= $batch->schema();

            if ($dropNullKeyRefs !== null) {
                $kept = [];

                foreach ($batch->all() as $row) {
                    foreach ($dropNullKeyRefs as $ref) {
                        if ($row->get($ref) === null) {
                            continue 2;
                        }
                    }

                    $kept[] = $row;
                }

                $batch = Rows::trusted($batch->schema(), $kept);

                if ($batch->empty()) {
                    continue;
                }
            }

            yield $batch;
        }
    }
}
