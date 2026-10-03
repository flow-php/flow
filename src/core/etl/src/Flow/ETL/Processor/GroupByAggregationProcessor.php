<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\BoundStep;
use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\HashBucketing;
use Flow\ETL\Dataset\Memory\BoundedRead;
use Flow\ETL\Dataset\Memory\MemoryBudget;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\AggregatedGroups;
use Flow\ETL\GroupBy\Group;
use Flow\ETL\GroupBy\GroupByShape;
use Flow\ETL\GroupBy\GroupKey;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;
use Generator;

use function array_values;
use function iterator_to_array;
use function serialize;

/**
 * Aggregates in one pass while the process stays under the memory limit. Past it, the rest of the stream is partitioned
 * into buckets and each bucket aggregated on its own; the groups aggregated before the limit are merged into the bucket
 * their key hashes to.
 */
final class GroupByAggregationProcessor implements Processor
{
    /**
     * The (input, aggregators, output) triple this step declares. Only bind() sets it; the unbound
     * path derives it from the first batch that carries rows.
     */
    private ?GroupByShape $shape = null;

    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(
        public readonly GroupBy $groupBy,
        public readonly HashBucketing $bucketing,
        public readonly Buckets $buckets,
        public readonly Unit $memoryLimit,
        public readonly int $batchSize = 1000,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->batchSize);
        }
    }

    /**
     * @throws SchemaDefinitionNotFoundException
     */
    public function bind(Schema $input): BoundStep
    {
        $bound = new self($this->groupBy, $this->bucketing, $this->buckets, $this->memoryLimit, $this->batchSize);
        $bound->shape = GroupByShape::of($this->groupBy, $input);

        return new BoundStep($bound, $bound->shape->output);
    }

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function process(Generator $rows, FlowContext $context): Generator
    {
        $shape = $this->shape;
        $groups = null;
        $budget = new MemoryBudget($context->backend(), $this->memoryLimit);
        $spilling = false;

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
                $groups->accumulate($batch, $context);

                if ($budget->exceeded()) {
                    $spilling = true;

                    break;
                }
            }

            if ($spilling && $groups !== null && $shape !== null) {
                yield from $this->spilled($groups, $shape, $rows, $context);

                return;
            }

            if ($groups !== null) {
                yield from $groups->flush($this->batchSize, $context);

                return;
            }

            // SQL's scalar aggregate: no input and no grouping key is one row of the initial accumulators,
            // not no row. Only reachable bound - an unbound plan has no schema to type the defaults with.
            if ($shape !== null && $this->groupBy->isGlobal()) {
                yield (new RowsBuilder($shape->output, $context->backend()))->appendRows([
                    $this->groupBy->aggregatedValues(
                        new GroupKey([], serialize([])),
                        $shape->aggregators->cloned(),
                        $shape->output,
                    ),
                ])->finish();
            }
        } finally {
            $this->buckets->clear();
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
        FlowContext $context,
    ): Generator {
        foreach ($this->bucketing->bucketize(
            (new BoundedRead($this->memoryLimit, $context->backend()))->followedBy([], $rest),
            $this->buckets->storage(),
        ) as $bucket) {
            $this->buckets->add($bucket);
        }

        /** @var array<int, array<string, Group>> $partials */
        $partials = [];

        foreach ($earlier->groups() as $key => $group) {
            $partials[$this->bucketing->indexOf(array_values(iterator_to_array($group->key)))][$key] = $group;
        }

        foreach ($this->buckets->all() as $bucket) {
            $groups = new AggregatedGroups($this->groupBy, $shape);

            foreach ($this->buckets->rows($bucket->id) as $batch) {
                $groups->accumulate($batch, $context);
            }

            foreach ($partials[$bucket->index] ?? [] as $key => $group) {
                $groups->absorbEarlier($key, $group, $context);
            }

            unset($partials[$bucket->index]);

            yield from $groups->flush($this->batchSize, $context);
        }

        // groups whose bucket received no row after the limit
        $remaining = new AggregatedGroups($this->groupBy, $shape);

        foreach ($partials as $groups) {
            foreach ($groups as $key => $group) {
                $remaining->absorbEarlier($key, $group, $context);
            }
        }

        yield from $remaining->flush($this->batchSize, $context);
    }
}
