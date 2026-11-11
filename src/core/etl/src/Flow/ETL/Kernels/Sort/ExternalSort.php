<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Sort;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Progress;
use Flow\ETL\Foundation\RandomValueGenerator;
use Flow\ETL\Foundation\VoidProgress;
use Flow\ETL\Kernels\Bucketing\Bucket;
use Flow\ETL\Kernels\Bucketing\BucketRun;
use Flow\ETL\Kernels\Bucketing\Buckets;
use Flow\ETL\Kernels\Sort\Merge\KWayMerge;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Memory\MemoryBudget;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Sort\SortColumn;
use Generator;

use function array_chunk;
use function array_map;
use function array_splice;
use function count;
use function intdiv;
use function max;
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
        public int $mergeFanIn = 10,
        public BatchSize $batchSize = new BatchSize(1000),
        private Progress $progress = new VoidProgress(),
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
    public static function of(
        array $columns,
        ExternalSortConfig $config,
        RandomValueGenerator $random,
        Progress $progress = new VoidProgress(),
    ): self {
        return new self(
            $columns,
            new Buckets($config->bucketing->storage),
            // without the ??, a merge side built on its own would silently spill to local disk for a MemoryBuckets user
            new Buckets($config->merge ?? $config->bucketing->storage),
            $random,
            $config->bucketing->bucketsCount,
            new BatchSize($config->bucketing->batchSize),
            $progress,
        );
    }

    /**
     * @param Generator<Rows> $rows
     * @param null|int $limit
     *
     * @return Generator<Rows>
     */
    public function sort(Generator $rows, Backend $backend, MemoryBudget $budget, ?int $limit = null): Generator
    {
        $runId = $this->random->string(16);

        /** @var list<Rows> $buffer */
        $buffer = [];

        /** @var list<BucketRun> $runs */
        $runs = [];
        $schema = null;

        try {
            // drained in the try so a failure frees the runs; valid()/next(): the owner's first pull may end the stream
            for (; $rows->valid(); $rows->next()) {
                /** @var Rows $batch valid() holds, so current() is a batch */
                $batch = $rows->current();

                if ($batch->isEmpty()) {
                    continue;
                }

                $schema ??= $batch->schema();
                $buffer[] = $batch->matchTo($schema, $backend);

                // the spill concats the buffer, so it needs room for a second copy of it
                if ($budget->exceededWith(2)) {
                    // spliced out, so neither the buffer nor its concat outlives the spill
                    $runs = [
                        ...$runs,
                        ...$this->runs(
                            array_splice($buffer, 0),
                            $schema,
                            $backend,
                            $budget,
                            $limit,
                            $runId,
                            count($runs),
                        ),
                    ];
                    $budget->released();
                }
            }

            if ($schema === null) {
                return;
            }

            if ($runs === [] && $this->fitting($buffer, $backend, $budget) === count($buffer)) {
                yield from $this->emitted($this->sorted(
                    Rows::concat($schema, $backend, ...array_splice($buffer, 0)),
                    $limit,
                    $backend,
                )->chunks($this->batchSize->size));

                return;
            }

            $runs = [
                ...$runs,
                ...$this->runs(array_splice($buffer, 0), $schema, $backend, $budget, $limit, $runId, count($runs)),
            ];

            $merger = new KWayMerge($this->columns, $backend, $this->batchSize);
            $mergedIndex = 0;
            $fanIn = $this->fanIn($runs[0], $budget);

            // each pass merges neighbouring runs in place, so equal keys keep the order they arrived in
            while (count($runs) > $fanIn) {
                $runs = array_map(
                    fn(array $group): BucketRun => $this->reduce($group, $merger, $mergedIndex++, $limit),
                    array_chunk($runs, $fanIn),
                );
            }

            yield from $this->emitted($this->firstRows(
                $merger->merge(array_map(static fn(BucketRun $run): Generator => $run->rows(), $runs)),
                $limit,
            ));
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
     *
     * @return Generator<int, Rows>
     */
    public function counted(Generator $batches): Generator
    {
        // valid()/next(), never a rewind: top-n hands over a stream it already pulled from
        for (; $batches->valid(); $batches->next()) {
            /** @var Rows $batch valid() holds, so current() is a batch */
            $batch = $batches->current();
            $this->progress->advanced($batch->count());

            yield $batch;
        }
    }

    /**
     * @param Generator<Rows> $batches
     *
     * @return Generator<int, Rows>
     */
    private function emitted(Generator $batches): Generator
    {
        foreach ($batches as $batch) {
            $this->progress->busy();

            yield $batch;
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
     * @return int<2, max>
     */
    public function fanIn(BucketRun $run, MemoryBudget $budget): int
    {
        $remaining = $budget->remaining();
        // held in a variable, so the decoded batch is still alive when the second measurement runs
        $stream = $run->rows();
        $stream->current();
        $perRun = $remaining - $budget->remaining();

        // never below 2: an exhausted budget takes more merge passes instead of failing
        return max(2, $perRun < 1 ? $this->mergeFanIn : min($this->mergeFanIn, intdiv($remaining, $perRun)));
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
            $this->progress->busy();
        }

        foreach ($group as $run) {
            $run->remove();
        }

        $this->merge->add(new Bucket($bucketId, $totalRows, $index));

        return new BucketRun($bucketId, $this->merge);
    }

    /**
     * @param list<Rows> $batches
     * @param null|int $limit
     *
     * @return list<BucketRun>
     */
    public function runs(
        array $batches,
        Schema $schema,
        Backend $backend,
        MemoryBudget $budget,
        ?int $limit,
        string $runId,
        int $index,
    ): array {
        $runs = [];

        // spliced a run at a time, so the batches already spilled free the room the next run's sort needs
        while ($batches !== []) {
            $runs[] = $this->spill(
                $this->sorted(
                    Rows::concat(
                        $schema,
                        $backend,
                        ...array_splice($batches, 0, $this->fitting($batches, $backend, $budget)),
                    ),
                    $limit,
                    $backend,
                ),
                $runId,
                $index + count($runs),
            );
        }

        return $runs;
    }

    /**
     * @param list<Rows> $batches
     *
     * @return int the leading batches whose sort stays under the process cap, or the floor once over it, at least one
     */
    public function fitting(array $batches, Backend $backend, MemoryBudget $budget): int
    {
        $bytes = 0;

        foreach ($batches as $count => $batch) {
            $bytes += $backend->sortWorkingBytes($batch, $this->columns);

            if (
                $count > 0
                && ($budget->crossesCapWith(0) ? $bytes > $budget->floor() : $budget->crossesCapWith($bytes))
            ) {
                return $count;
            }
        }

        return count($batches);
    }

    /**
     * @param null|int $limit
     */
    public function sorted(Rows $all, ?int $limit, Backend $backend): Rows
    {
        $sorted = $backend->sort($all, $this->columns);

        return $limit === null ? $sorted : $sorted->take($limit);
    }

    public function spill(Rows $run, string $runId, int $index): BucketRun
    {
        $bucketId = sprintf('sort-%s-%d', $runId, $index);
        // one frame per batch, so a merge cursor decodes one batch of the run at a time
        foreach ($run->chunks($this->batchSize->size) as $chunk) {
            $this->spill->storage()->append($bucketId, $chunk);
            $this->progress->busy();
        }

        $this->spill->storage()->close($bucketId);
        $this->spill->add(new Bucket($bucketId, $run->count(), $index));

        return new BucketRun($bucketId, $this->spill);
    }
}
