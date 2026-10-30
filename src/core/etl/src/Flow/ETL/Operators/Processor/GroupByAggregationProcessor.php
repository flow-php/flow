<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Foundation\RandomValueGenerator;
use Flow\ETL\Kernels\Bucketing\Buckets;
use Flow\ETL\Kernels\Bucketing\HashBucketing;
use Flow\ETL\Kernels\Bucketing\KeyEncoder;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\AggregatedGroups;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Operators\GroupBy\Group;
use Flow\ETL\Operators\GroupBy\GroupByShape;
use Flow\ETL\Operators\GroupBy\GroupKey;
use Flow\ETL\Operators\GroupBy\MergedAggregate;
use Flow\ETL\Operators\GroupBy\PartialAggregate;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Memory\BoundedRead;
use Flow\ETL\Values\Memory\MemoryBudget;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_instance_of;
use function serialize;

/**
 * Aggregates in one pass while the process stays under the memory limit. Past it, the rest of the stream is partitioned
 * into buckets and each bucket aggregated on its own; the groups aggregated before the limit are merged into the bucket
 * their key hashes to.
 */
final readonly class GroupByAggregationProcessor implements Distributable
{
    /**
     * @param BucketingConfig $config the effective grouping config: the operation's own or the frame's
     */
    public function __construct(
        public GroupBy $groupBy,
        public BucketingConfig $config,
        private RandomValueGenerator $random,
        private ?GroupByShape $shape = null,
    ) {}

    /**
     * @throws SchemaDefinitionNotFoundException
     */
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $groupBy = $rebuild->of($data['group_by'] ?? null, GroupBy::class);

        return new self(
            $groupBy,
            $rebuild->of($data['config'] ?? null, BucketingConfig::class),
            type_instance_of(RandomValueGenerator::class)->assert($rebuild->of(
                $data['random'] ?? null,
                Describable::class,
            )),
            ($data['input'] ?? null) === null
                ? null
                : GroupByShape::of($groupBy, Schema::fromArray(type_array()->assert($data['input']))),
        );
    }

    /**
     * null when the random value generator or the grouping config does not describe.
     */
    public function describe(): ?Description
    {
        return (
            $this->random instanceof Describable
                ? Description::compose(self::class, [
                    'group_by' => $this->groupBy,
                    'config' => $this->config,
                    'random' => $this->random,
                    'input' => $this->shape?->input->normalize(),
                ])
                : null
        );
    }

    /**
     * Every split aggregates its rows into partials (PartialAggregate); the partials of one key are merged
     * (MergedAggregate) in one process. null while unbound, for a pivot, or when it does not describe.
     *
     * @throws SchemaDefinitionNotFoundException
     */
    public function distribution(): ?Distribution
    {
        if ($this->shape === null || $this->groupBy->isPivot() || $this->describe() === null) {
            return null;
        }

        $partials = [];

        foreach ($this->groupBy->aggregations() as $aggregation) {
            $partials[] = new PartialAggregate($aggregation);
        }

        $merged = [];

        foreach ($this->shape->aggregators as $aggregation) {
            $merged[] = new MergedAggregate($aggregation);
        }

        $partial = new GroupBy($this->groupBy->refs(), new Aggregators(...$partials));
        $partialSchema = $partial->outputSchema(
            $this->shape->input,
            $partial->aggregations()->resolved($this->shape->input),
        );
        $partialStep = (new self($partial, $this->config, $this->random))->bind($this->shape->input)->step;
        $finalStep = (new self(
            new GroupBy($this->groupBy->refs(), new Aggregators(...$merged)),
            $this->config,
            $this->random,
        ))->bind($partialSchema)->step;

        $keys = $this->groupBy->refs()->names();

        return $keys === []
            ? Distribution::single($partialStep, $finalStep)
            : Distribution::clustered($keys, $partialStep, $finalStep);
    }

    /**
     * The bound shape; null while unbound.
     */
    public function shape(): ?GroupByShape
    {
        return $this->shape;
    }

    /**
     * @throws SchemaDefinitionNotFoundException
     */
    public function bind(Schema $input): BoundStep
    {
        $shape = GroupByShape::of($this->groupBy, $input);

        return new BoundStep(new self($this->groupBy, $this->config, $this->random, $shape), $shape->output);
    }

    public function breaks(): bool
    {
        return true;
    }

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function process(Generator $rows, Backend $backend): Generator
    {
        $shape = $this->shape;
        $groups = null;
        $budget = new MemoryBudget($backend, $this->config->memoryLimit);
        $spilling = false;
        $buckets = new Buckets($this->config->spilling($backend));

        try {
            while ($rows->valid()) {
                /** @var Rows $batch valid() holds, so current() is a batch */
                $batch = $rows->current();
                $rows->next();

                if ($batch->isEmpty()) {
                    continue;
                }

                $shape ??= GroupByShape::of($this->groupBy, $batch->schema());
                $groups ??= new AggregatedGroups($this->groupBy, $shape);
                $groups->accumulate($batch, $backend);

                if ($budget->exceeded()) {
                    $spilling = true;

                    break;
                }
            }

            if ($spilling && $groups !== null && $shape !== null) {
                yield from $this->spilled($groups, $shape, $rows, $buckets, $backend);

                return;
            }

            if ($groups !== null) {
                yield from $groups->flush($this->config->batchSize, $backend);

                return;
            }

            // SQL's scalar aggregate: no input and no grouping key is one row of the initial accumulators,
            // not no row. Only reachable bound - an unbound plan has no schema to type the defaults with.
            if ($shape !== null && $this->groupBy->isGlobal()) {
                yield (new AggregatedGroups($this->groupBy, $shape))->rows([new Group(
                    new GroupKey([], serialize([])),
                    $shape->aggregators->fresh(),
                )], $backend);
            }
        } finally {
            $buckets->clear();
        }
    }

    /**
     * @param Generator<Rows> $rest the stream after the batch that passed the limit
     *
     * @return Generator<Rows>
     */
    public function spilled(
        AggregatedGroups $earlier,
        GroupByShape $shape,
        Generator $rest,
        Buckets $buckets,
        Backend $backend,
    ): Generator {
        $bucketing = new HashBucketing(
            new KeyEncoder($this->groupBy->refs()->names(), declared: $this->shape?->input),
            $this->config->bucketsCount,
            $this->random,
            'group-by',
        );

        foreach ($bucketing->bucketize(BoundedRead::followedBy([], $rest), $buckets->storage()) as $bucket) {
            $buckets->add($bucket);
        }

        /** @var array<int, array<string, Group>> $partials */
        $partials = [];

        foreach ($earlier->groups() as $key => $group) {
            $partials[$bucketing->indexOf($key)][$key] = $group;
        }

        foreach ($buckets->all() as $bucket) {
            $groups = new AggregatedGroups($this->groupBy, $shape);

            foreach ($buckets->rows($bucket->id) as $batch) {
                $groups->accumulate($batch, $backend);
            }

            foreach ($partials[$bucket->index] ?? [] as $key => $group) {
                $groups->absorbEarlier($key, $group, $backend);
            }

            unset($partials[$bucket->index]);

            yield from $groups->flush($this->config->batchSize, $backend);
        }

        // groups whose bucket received no row after the limit
        $remaining = new AggregatedGroups($this->groupBy, $shape);

        foreach ($partials as $groups) {
            foreach ($groups as $key => $group) {
                $remaining->absorbEarlier($key, $group, $backend);
            }
        }

        yield from $remaining->flush($this->config->batchSize, $backend);
    }
}
