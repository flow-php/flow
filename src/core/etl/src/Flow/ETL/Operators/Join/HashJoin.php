<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Join;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\JoinException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\Foundation\RandomValueGenerator;
use Flow\ETL\Functions\Join\Expression;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Kernels\Bucketing\Bucket;
use Flow\ETL\Kernels\Bucketing\Buckets;
use Flow\ETL\Kernels\Bucketing\HashBucketing;
use Flow\ETL\Kernels\Bucketing\KeyEncoder;
use Flow\ETL\Kernels\HashJoin\Joiner;
use Flow\ETL\Kernels\HashJoin\JoinSide;
use Flow\ETL\Kernels\HashJoin\NullKeyFilter;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Processor\Distribution;
use Flow\ETL\Storage\ResidentRowsStorage;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Join\JoinType;
use Flow\ETL\Values\Join\Side;
use Flow\ETL\Values\Memory\BoundedRead;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

use function array_intersect_key;
use function array_keys;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;

final readonly class HashJoin implements Describable
{
    /**
     * @param HashJoinConfig $config the effective join config: the operation's own or the frame's
     */
    public function __construct(
        public Expression $expression,
        public JoinType $type,
        public HashJoinConfig $config,
        private RandomValueGenerator $random,
        private ?Schema $left = null,
        private ?Schema $declaredRight = null,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['expression'] ?? null, Expression::class),
            JoinType::from(type_string()->assert($data['type'] ?? null)),
            $rebuild->of($data['config'] ?? null, HashJoinConfig::class),
            type_instance_of(RandomValueGenerator::class)->assert($rebuild->of(
                $data['random'] ?? null,
                Describable::class,
            )),
            ($data['left'] ?? null) === null ? null : Schema::fromArray(type_array()->assert($data['left'])),
            ($data['right'] ?? null) === null ? null : Schema::fromArray(type_array()->assert($data['right'])),
        );
    }

    /**
     * null when the random value generator or the join config does not describe.
     */
    public function describe(): ?Description
    {
        return (
            $this->random instanceof Describable
                ? Description::compose(self::class, [
                    'expression' => $this->expression,
                    'type' => $this->type->value,
                    'config' => $this->config,
                    'random' => $this->random,
                    'left' => $this->left?->normalize(),
                    'right' => $this->declaredRight?->normalize(),
                ])
                : null
        );
    }

    /**
     * The join with $right as the right side's declared schema, for a join that runs unbound: a left or anti join over
     * an empty right side still pads the right columns.
     */
    public function withRightSchema(Schema $right): self
    {
        return new self($this->expression, $this->type, $this->config, $this->random, $this->left, $right);
    }

    /**
     * coPartitioned on the equality keys; null for a non-equality join, or while either side is unbound.
     */
    public function distribution(): ?Distribution
    {
        $keys = $this->expression->keys();

        return $keys === null || $this->leftSchema() === null || $this->rightSchema() === null
            ? null
            : Distribution::coPartitioned($keys);
    }

    /**
     * The pair kernel of this join: matches one left and one right stream already split by key.
     */
    public function joiner(Backend $backend): Joiner
    {
        return new Joiner(
            $this->type,
            $this->expression->schema($this->type),
            $this->expression->keys(),
            $this->expression,
            $backend,
            new BatchSize($this->config->bucketing->batchSize),
        );
    }

    /**
     * The bound left schema; null unbound.
     */
    public function leftSchema(): ?Schema
    {
        return $this->left;
    }

    /**
     * The bound right schema; null unbound.
     */
    public function rightSchema(): ?Schema
    {
        return $this->declaredRight;
    }

    /**
     * @return BoundStep<self>
     */
    public function bind(Schema $left, Schema $right): BoundStep
    {
        return new BoundStep(
            new self($this->expression, $this->type, $this->config, $this->random, $left, $right),
            $this->expression->schema($this->type)->output($left, $right),
        );
    }

    /**
     * Needs every right row before it emits: the join ends its pipeline. A side whose schema is not declared is
     * discovered from the rows that flow.
     *
     * @param Generator<Rows> $left
     * @param Generator<Rows> $right
     *
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function join(Generator $left, Generator $right, Backend $backend): Generator
    {
        $leftSchema = $this->left;
        $rightSchema = $this->declaredRight;

        $equalityKeys = $this->expression->keys();
        $joiner = $this->joiner($backend);
        $leftBuckets = new Buckets($this->config->bucketing->spilling($backend));
        $rightBuckets = new Buckets($this->config->bucketing->spilling($backend));
        $resident = $rightBuckets->storage() instanceof ResidentRowsStorage;

        try {
            $rightRows = $this->tap($right, $rightSchema);

            // right rows with a null join key can never match, they only surface in right join output
            if ($equalityKeys !== null && $this->type->dropsNullKeys(Side::right)) {
                $rightRows = (new NullKeyFilter($equalityKeys->right))->rows($rightRows);
            }

            /** @var null|list<Rows> $held the build side, when all of it fits under the memory limit */
            $held = null;

            if (!$resident) {
                [$read, $fits] = (new BoundedRead($this->config->bucketing->memoryLimit, $backend))->read($rightRows);

                if ($fits) {
                    $held = $read;
                } else {
                    $rightRows = BoundedRead::followedBy($read, $rightRows);
                }
            }

            // the bounded read has seen the first right batch, so an undescribable right side has its schema now
            [$leftKeys, $rightKeys] = $equalityKeys?->encoders($leftSchema, $rightSchema) ?? [null, null];

            if ($held === null) {
                $this->bucketize($rightRows, $rightBuckets, $resident ? null : $rightKeys, 'join-right');
            }

            $leftRows = $this->tap($left, $leftSchema);

            // left rows with a null join key can never match, they only surface in left/left_anti output
            if ($equalityKeys !== null && $this->type->dropsNullKeys(Side::left)) {
                $leftRows = (new NullKeyFilter($equalityKeys->left))->rows($leftRows);
            }

            if ($resident || $held !== null) {
                $rightBucket = $rightBuckets->all()[0] ?? null;

                $joinedBatches = $joiner->join(JoinSide::of($leftRows, $leftSchema), JoinSide::of(match (true) {
                    $held !== null => BoundedRead::followedBy($held, self::noRows()),
                    $rightBucket === null => self::noRows(),
                    default => $rightBuckets->rows($rightBucket->id),
                }, $rightSchema));

                // the left side streams through the join, so a stop has to reach its source: the upstream
                // run then completes and closes its loaders instead of being abandoned mid-run
                foreach ($joinedBatches as $joinedBatch) {
                    if ((yield $joinedBatch) === Signal::STOP) {
                        $left->send(Signal::STOP);

                        return;
                    }
                }

                return;
            }

            $this->bucketize($leftRows, $leftBuckets, $leftKeys, 'join-left');

            foreach ($this->bucketPairs($leftBuckets, $rightBuckets) as [$leftBucket, $rightBucket]) {
                // a bucket pair is a slice of each side; the output shape is the whole side's
                $joinedBatches = $joiner->join(
                    JoinSide::of(
                        $leftBucket === null ? self::noRows() : $leftBuckets->rows($leftBucket->id),
                        $leftSchema,
                    ),
                    JoinSide::of(
                        $rightBucket === null ? self::noRows() : $rightBuckets->rows($rightBucket->id),
                        $rightSchema,
                    ),
                    buildLeft: ($leftBucket->totalRows ?? 0) < ($rightBucket->totalRows ?? 0),
                );

                // re-key batches, yield from would restart keys at 0 for every bucket pair
                foreach ($joinedBatches as $joinedBatch) {
                    yield $joinedBatch;
                }
            }
        } catch (SchemaDefinitionNotUniqueException $e) {
            throw new JoinException($e->getMessage(), (int) $e->getCode(), $e);
        } finally {
            $leftBuckets->clear();
            $rightBuckets->clear();
        }
    }

    /**
     * @param Generator<Rows> $rows
     * @param null|KeyEncoder $keys - null puts every row in a single bucket (non-equality join / resident side)
     */
    private function bucketize(Generator $rows, Buckets $buckets, ?KeyEncoder $keys, string $namespace): void
    {
        $strategy = new HashBucketing(
            $keys ?? new KeyEncoder([]),
            $keys === null ? 1 : $this->config->bucketing->bucketsCount,
            $this->random,
            $namespace,
        );

        foreach ($strategy->bucketize($rows, $buckets->storage()) as $bucket) {
            $buckets->add($bucket);
        }
    }

    /**
     * A bucket that received no rows was never created, so a missing side is an empty side.
     *
     * @return Generator<array{?Bucket, ?Bucket}>
     */
    private function bucketPairs(Buckets $left, Buckets $right): Generator
    {
        /** @var array<int, Bucket> $leftBuckets */
        $leftBuckets = [];

        foreach ($left->all() as $bucket) {
            $leftBuckets[$bucket->index] = $bucket;
        }

        /** @var array<int, Bucket> $rightBuckets */
        $rightBuckets = [];

        foreach ($right->all() as $bucket) {
            $rightBuckets[$bucket->index] = $bucket;
        }

        $indexes = match ($this->type) {
            JoinType::inner => array_keys(array_intersect_key($leftBuckets, $rightBuckets)),
            JoinType::left, JoinType::left_anti => array_keys($leftBuckets),
            JoinType::right => array_keys($rightBuckets),
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
     * @param Generator<Rows> $rows
     * @param null|Schema $schema - out parameter, the schema of the first batch flowing through
     *
     * @return Generator<Rows>
     */
    private function tap(Generator $rows, ?Schema &$schema): Generator
    {
        foreach ($rows as $batch) {
            $schema ??= $batch->schema();

            yield $batch;
        }
    }
}
