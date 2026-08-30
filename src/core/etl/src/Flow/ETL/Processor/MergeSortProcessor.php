<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Bucketing\Bucket;
use Flow\ETL\Bucketing\BucketRun;
use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\BucketShape;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\RandomValueGenerator;
use Flow\ETL\Row\References;
use Flow\ETL\Sort\Merge\KWayMerge;
use Generator;

use function array_slice;
use function array_splice;
use function count;

final class MergeSortProcessor implements Processor
{
    /**
     * @param int<1, max> $mergeFanIn
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private readonly References $refs,
        private readonly Buckets $spill,
        private readonly Buckets $merge,
        private readonly RandomValueGenerator $random,
        private readonly int $mergeFanIn = 10,
        private readonly int $batchSize = 1000,
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

    public function process(Generator $rows, FlowContext $context): Generator
    {
        /** @var list<BucketRun> $runs */
        $runs = [];

        try {
            // draining upstream inside the try is what makes a mid-stream failure release the spilled runs
            foreach ($rows as $batch) {
                foreach ($batch as $row) {
                    /** @var string $bucketId */
                    $bucketId = $row->get(BucketShape::id->value);
                    $runs[] = new BucketRun($bucketId, $this->spill);
                }
            }

            $merger = new KWayMerge($this->refs, $this->batchSize);
            $mergedIndex = 0;

            while (count($runs) > $this->mergeFanIn) {
                $runs[] = $this->reduce(array_slice($runs, 0, $this->mergeFanIn), $merger, $mergedIndex++);
                array_splice($runs, 0, $this->mergeFanIn);
            }

            yield from $merger->merge($runs);
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
     * @param list<BucketRun> $group
     *
     * @return BucketRun the merged run, bound to the merge storage that wrote it
     */
    private function reduce(array $group, KWayMerge $merger, int $index): BucketRun
    {
        $bucketId = 'sort-merge-' . $this->random->string(16);

        // registered before the spill so clear() covers a partially written merged run
        $this->merge->add(new Bucket($bucketId, 0, $index));
        $totalRows = 0;

        foreach ($merger->merge($group) as $batch) {
            $this->merge->storage()->append($bucketId, $batch);
            $totalRows += $batch->count();
        }

        foreach ($group as $run) {
            $run->remove();
        }

        $this->merge->add(new Bucket($bucketId, $totalRows, $index));

        return new BucketRun($bucketId, $this->merge);
    }
}
