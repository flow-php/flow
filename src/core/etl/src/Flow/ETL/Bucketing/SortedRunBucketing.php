<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\RandomValueGenerator;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Generator;

final class SortedRunBucketing implements BucketingStrategy
{
    public function __construct(
        private readonly References $refs,
        private readonly int $runSize,
        private readonly RandomValueGenerator $random,
    ) {
        if ($this->runSize < 1) {
            throw new InvalidArgumentException('Run size must be greater than 0, given: ' . $this->runSize);
        }
    }

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Bucket>
     */
    public function bucketize(Generator $rows, BucketsStorage $storage): Generator
    {
        $buffer = new Rows();
        $index = 0;

        foreach ($rows as $batch) {
            $buffer = $buffer->merge($batch);

            while ($buffer->count() >= $this->runSize) {
                yield $this->spill($buffer->take($this->runSize), $storage, $index++);
                $buffer = $buffer->drop($this->runSize);
            }
        }

        if (!$buffer->empty()) {
            yield $this->spill($buffer, $storage, $index);
        }
    }

    private function spill(Rows $run, BucketsStorage $storage, int $index): Bucket
    {
        $bucketId = $this->random->string(32);
        $storage->set($bucketId, $run->sortBy(...$this->refs->all()));

        return new Bucket($bucketId, $run->count(), $index);
    }
}
