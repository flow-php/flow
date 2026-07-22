<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Bucketing\Bucket;
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
        private readonly Buckets $buckets,
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
        /** @var list<string> $bucketIds */
        $bucketIds = [];

        foreach ($rows as $batch) {
            foreach ($batch as $row) {
                /** @var string $bucketId */
                $bucketId = $row->valueOf(BucketShape::id->value);
                $bucketIds[] = $bucketId;
            }
        }

        $merger = new KWayMerge($this->buckets->storage(), $this->refs, $this->batchSize);

        try {
            $mergedIndex = 0;

            while (count($bucketIds) > $this->mergeFanIn) {
                $bucketIds[] = $this->reduce(array_slice($bucketIds, 0, $this->mergeFanIn), $merger, $mergedIndex++);
                array_splice($bucketIds, 0, $this->mergeFanIn);
            }

            yield from $merger->merge($bucketIds);
        } finally {
            $this->buckets->clear();
        }
    }

    /**
     * @param list<string> $group
     *
     * @return string id of the bucket holding the merged runs
     */
    private function reduce(array $group, KWayMerge $merger, int $index): string
    {
        $bucketId = 'sort-merge-' . $this->random->string(16);

        // registered before the spill so clear() covers a partially written merged run
        $this->buckets->add(new Bucket($bucketId, 0, $index));
        $totalRows = 0;

        foreach ($merger->merge($group) as $batch) {
            $this->buckets->storage()->append($bucketId, $batch);
            $totalRows += $batch->count();
        }

        foreach ($group as $id) {
            $this->buckets->remove($id);
        }

        $this->buckets->add(new Bucket($bucketId, $totalRows, $index));

        return $bucketId;
    }
}
