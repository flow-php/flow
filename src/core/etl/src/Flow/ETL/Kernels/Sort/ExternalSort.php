<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Sort;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\RandomValueGenerator;
use Flow\ETL\Kernels\Bucketing\Bucket;
use Flow\ETL\Kernels\Bucketing\BucketRun;
use Flow\ETL\Kernels\Bucketing\Buckets;
use Flow\ETL\Kernels\Sort\Merge\KWayMerge;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Memory\MemoryBudget;
use Flow\ETL\Values\Memory\Unit;
use Flow\ETL\Values\Rows;
use Generator;

use function array_chunk;
use function array_map;
use function count;
use function min;
use function sprintf;

/**
 * With a limit, every run and merge stops at $limit rows.
 */
final readonly class ExternalSort
{
    /**
     * @param list<SortColumn> $columns
     * @param int<1, max> $mergeFanIn
     */
    public function __construct(
        public array $columns,
        public Buckets $spill,
        public Buckets $merge,
        private RandomValueGenerator $random,
        public Unit $memoryLimit,
        public int $mergeFanIn = 10,
        public BatchSize $batchSize = new BatchSize(1000),
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->mergeFanIn < 1) {
            throw new InvalidArgumentException('Merge fan-in must be greater than 0, given: ' . $this->mergeFanIn);
        }
    }

    /**
     * @param list<SortColumn> $columns
     */
    public static function of(array $columns, ExternalSortConfig $config, RandomValueGenerator $random): self
    {
        return new self(
            $columns,
            new Buckets($config->bucketing->storage),
            // without the ??, a merge side built on its own would silently spill to local disk for a MemoryBuckets user
            new Buckets($config->merge ?? $config->bucketing->storage),
            $random,
            $config->bucketing->memoryLimit,
            $config->bucketing->bucketsCount,
            new BatchSize($config->bucketing->batchSize),
        );
    }

    /**
     * @param Generator<Rows> $rows
     * @param null|int $limit
     *
     * @return Generator<Rows>
     */
    public function sort(Generator $rows, Backend $backend, ?int $limit = null): Generator
    {
        $budget = new MemoryBudget($backend, $this->memoryLimit);
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
                $buffer[] = $batch->matchTo($schema, $backend);

                // the whole process, not this buffer: another spilling step in the pipeline holds memory too
                if ($budget->exceeded()) {
                    $runs[] = $this->spill($this->sorted($buffer, $limit, $backend), $runId, count($runs));
                    $buffer = [];
                    $budget->released();
                }
            }

            if ($runs === []) {
                if ($buffer !== []) {
                    yield from $this->sorted($buffer, $limit, $backend)->chunks($this->batchSize->size);
                }

                return;
            }

            if ($buffer !== []) {
                $runs[] = $this->spill($this->sorted($buffer, $limit, $backend), $runId, count($runs));
            }

            $merger = new KWayMerge($this->columns, $backend, $this->batchSize);
            $mergedIndex = 0;

            // each pass merges neighbouring runs in place, so equal keys keep the order they arrived in
            while (count($runs) > $this->mergeFanIn) {
                $runs = array_map(
                    fn(array $group): BucketRun => $this->reduce($group, $merger, $mergedIndex++, $limit),
                    array_chunk($runs, $this->mergeFanIn),
                );
            }

            yield from $this->firstRows(
                $merger->merge(array_map(static fn(BucketRun $run): Generator => $run->rows(), $runs)),
                $limit,
            );
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

        foreach ($this->firstRows(
            $merger->merge(array_map(static fn(BucketRun $run): Generator => $run->rows(), $group)),
            $limit,
        ) as $batch) {
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
        $all = Rows::concat($buffer[0]->schema(), $backend, ...$buffer);
        $order = new RowOrder($this->columns);
        $sorted = $all->gather($order->permutation($order->keys($all), $all->count()));

        return $limit === null ? $sorted : $sorted->take($limit);
    }

    public function spill(Rows $run, string $runId, int $index): BucketRun
    {
        $bucketId = sprintf('sort-%s-%d', $runId, $index);
        $this->spill->storage()->append($bucketId, $run);
        $this->spill->storage()->close($bucketId);
        $this->spill->add(new Bucket($bucketId, $run->count(), $index));

        return new BucketRun($bucketId, $this->spill);
    }
}
