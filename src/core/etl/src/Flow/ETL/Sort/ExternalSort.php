<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

use Flow\ETL\Bucketing\Bucket;
use Flow\ETL\Bucketing\BucketRun;
use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Column\Backend;
use Flow\ETL\Dataset\Memory\MemoryBudget;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\RandomValueGenerator;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Sort\Merge\KWayMerge;
use Generator;

use function array_chunk;
use function array_map;
use function array_slice;
use function count;
use function min;
use function sprintf;

/**
 * Sorts in memory while the process stays under the memory limit; past it, sorted runs are spilled and
 * block-merged. With a limit, only the first $limit rows are produced - every run and every merge stops there.
 */
final readonly class ExternalSort
{
    /**
     * @param int<1, max> $mergeFanIn
     * @param int<1, max> $batchSize
     */
    public function __construct(
        public References $refs,
        public Buckets $spill,
        public Buckets $merge,
        private RandomValueGenerator $random,
        public Unit $memoryLimit,
        public int $mergeFanIn = 10,
        public int $batchSize = 1000,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->mergeFanIn < 1) {
            throw new InvalidArgumentException('Merge fan-in must be greater than 0, given: ' . $this->mergeFanIn);
        }

        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->batchSize);
        }
    }

    /**
     * @param Generator<Rows> $rows
     * @param null|int $limit
     *
     * @return Generator<Rows>
     */
    public function sort(Generator $rows, FlowContext $context, ?int $limit = null): Generator
    {
        $budget = new MemoryBudget($context->backend(), $this->memoryLimit);
        $runId = $this->random->string(16);

        /** @var list<Rows> $buffer */
        $buffer = [];

        /** @var list<BucketRun> $runs */
        $runs = [];
        $schema = null;

        try {
            // draining upstream inside the try is what makes a mid-stream failure release the spilled runs
            foreach ($rows as $batch) {
                if ($batch->isEmpty()) {
                    continue;
                }

                $schema ??= $batch->schema();
                $buffer[] = $batch->matchTo($schema, $context->backend());

                // the whole process, not this buffer: another spilling step in the pipeline holds memory too
                if ($budget->exceeded()) {
                    $runs[] = $this->spill($this->sorted($buffer, $limit, $context->backend()), $runId, count($runs));
                    $buffer = [];
                    $budget->released();
                }
            }

            if ($runs === []) {
                if ($buffer !== []) {
                    yield from $this->sorted($buffer, $limit, $context->backend())->chunks($this->batchSize);
                }

                return;
            }

            if ($buffer !== []) {
                $runs[] = $this->spill($this->sorted($buffer, $limit, $context->backend()), $runId, count($runs));
            }

            $merger = new KWayMerge($this->refs, $context->backend(), $this->batchSize);
            $mergedIndex = 0;

            // each pass merges neighbouring runs in place, so equal keys keep the order they arrived in
            while (count($runs) > $this->mergeFanIn) {
                $runs = array_map(
                    fn(array $group): BucketRun => $this->reduce($group, $merger, $mergedIndex++, $limit),
                    array_chunk($runs, $this->mergeFanIn),
                );
            }

            yield from $this->firstRows($merger->merge($runs), $limit);
        } finally {
            // nested, so a throwing spill clear still leaves the merge storage cleared - and chains as previous
            try {
                $this->spill->clear();
            } finally {
                $this->merge->clear();
            }
        }
    }

    /**
     * @param Generator<Rows> $batches
     * @param null|int $limit
     *
     * @return Generator<Rows>
     */
    public function firstRows(Generator $batches, ?int $limit): Generator
    {
        $remaining = $limit;

        foreach ($batches as $batch) {
            if ($remaining === null) {
                yield $batch;

                continue;
            }

            yield $batch->slice(0, min($remaining, $batch->count()));
            $remaining -= min($remaining, $batch->count());

            if ($remaining === 0) {
                return;
            }
        }
    }

    /**
     * @param list<BucketRun> $group
     * @param null|int $limit
     *
     * @return BucketRun the merged run, bound to the merge storage that wrote it
     */
    public function reduce(array $group, KWayMerge $merger, int $index, ?int $limit): BucketRun
    {
        $bucketId = 'sort-merge-' . $this->random->string(16);

        // registered before the spill so clear() covers a partially written merged run
        $this->merge->add(new Bucket($bucketId, 0, $index));
        $totalRows = 0;

        foreach ($this->firstRows($merger->merge($group), $limit) as $batch) {
            $this->merge->storage()->append($bucketId, $batch);
            $totalRows += $batch->count();
        }

        foreach ($group as $run) {
            $run->remove();
        }

        $this->merge->add(new Bucket($bucketId, $totalRows, $index));

        return new BucketRun($bucketId, $this->merge);
    }

    /**
     * @param list<Rows> $buffer
     * @param null|int $limit
     */
    public function sorted(array $buffer, ?int $limit, Backend $backend): Rows
    {
        $sorted = $buffer[0]->concat($backend, ...array_slice($buffer, 1))->sortBy(...$this->refs->all());

        return $limit === null ? $sorted : $sorted->take($limit);
    }

    public function spill(Rows $run, string $runId, int $index): BucketRun
    {
        $bucketId = sprintf('sort-%s-%d', $runId, $index);
        $this->spill->storage()->set($bucketId, $run);
        $this->spill->add(new Bucket($bucketId, $run->count(), $index));

        return new BucketRun($bucketId, $this->spill);
    }
}
