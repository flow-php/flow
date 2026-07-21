<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Bucketing\BucketShape;
use Flow\ETL\Bucketing\BucketsStorage;
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
    public function __construct(
        private readonly References $refs,
        private readonly BucketsStorage $storage,
        private readonly RandomValueGenerator $random,
        private readonly int $mergeFanIn = 10,
        private readonly int $batchSize = 1000,
    ) {
        if ($this->mergeFanIn < 1) {
            throw new InvalidArgumentException('Merge fan-in must be greater than 0, given: ' . $this->mergeFanIn);
        }

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

        $merger = new KWayMerge($this->storage, $this->refs, $this->batchSize);

        try {
            while (count($bucketIds) > $this->mergeFanIn) {
                $bucketIds[] = $this->reduce(array_slice($bucketIds, 0, $this->mergeFanIn), $merger);
                array_splice($bucketIds, 0, $this->mergeFanIn);
            }

            yield from $merger->merge($bucketIds);
        } finally {
            foreach ($bucketIds as $bucketId) {
                $this->storage->remove($bucketId);
            }
        }
    }

    /**
     * @param list<string> $group
     *
     * @return string id of the bucket holding the merged runs
     */
    private function reduce(array $group, KWayMerge $merger): string
    {
        $bucketId = $this->random->string(32);

        foreach ($merger->merge($group) as $batch) {
            $this->storage->append($bucketId, $batch);
        }

        foreach ($group as $id) {
            $this->storage->remove($id);
        }

        return $bucketId;
    }
}
