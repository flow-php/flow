<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\RandomValueGenerator;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Generator;

use function sprintf;

final class SortedRunBucketing implements BucketingStrategy
{
    /**
     * @param list<Reference> $by
     * @param int<1, max> $runSize
     */
    public function __construct(
        private readonly array $by,
        private readonly int $runSize,
        private readonly RandomValueGenerator $random,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
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
        $runId = $this->random->string(16);
        $buffer = null;
        $index = 0;

        foreach ($rows as $batch) {
            $buffer = $buffer === null ? $batch : $buffer->merge($batch);

            while ($buffer->count() >= $this->runSize) {
                yield $this->spill($buffer->take($this->runSize), $storage, $runId, $index++);
                $buffer = $buffer->drop($this->runSize);
            }
        }

        if ($buffer !== null && !$buffer->empty()) {
            yield $this->spill($buffer, $storage, $runId, $index);
        }
    }

    private function spill(Rows $run, BucketsStorage $storage, string $runId, int $index): Bucket
    {
        $bucketId = sprintf('sort-%s-%d', $runId, $index);
        $storage->set($bucketId, $run->sortBy(...$this->by));

        return new Bucket($bucketId, $run->count(), $index);
    }
}
