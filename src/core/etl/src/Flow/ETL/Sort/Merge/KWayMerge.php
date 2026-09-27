<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\Merge;

use Flow\ETL\Bucketing\BucketRun;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\References;
use Flow\ETL\Row\RowsBuffer;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

final readonly class KWayMerge
{
    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private References $refs,
        private int $batchSize = 1000,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->batchSize);
        }
    }

    /**
     * @param list<BucketRun> $runs
     *
     * @return Generator<Rows>
     */
    public function merge(array $runs): Generator
    {
        $heap = new RowsMinHeap(...$this->refs->all());

        /** @var array<string, BucketCursor> $cursors */
        $cursors = [];
        $schema = null;

        foreach ($runs as $run) {
            $cursor = new BucketCursor($run->rows());

            if ($cursor->valid()) {
                $schema ??= $cursor->schema();
                $heap->push($cursor->current(), $run->id);
                $cursor->next();
                $cursors[$run->id] = $cursor;
            }
        }

        $buffer = new RowsBuffer($schema ?? new Schema(), $this->batchSize);

        while (!$heap->isEmpty()) {
            $top = $heap->extract();

            if (null !== ($batch = $buffer->add($top->row))) {
                yield $batch;
            }

            $cursor = $cursors[$top->bucketId];

            if ($cursor->valid()) {
                $heap->push($cursor->current(), $top->bucketId);
                $cursor->next();
            }
        }

        if (null !== ($batch = $buffer->flush())) {
            yield $batch;
        }
    }
}
