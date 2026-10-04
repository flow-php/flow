<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\Merge;

use Flow\ETL\Bucketing\BucketRun;
use Flow\ETL\Column\Backend;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Sort\RowOrder;
use Generator;

use function array_filter;
use function array_slice;
use function array_values;

final readonly class KWayMerge
{
    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private References $refs,
        private Backend $backend,
        private int $batchSize = 1000,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->batchSize);
        }
    }

    /**
     * Merges block by block: every loaded row that sorts at or before the smallest last loaded row (the frontier) is
     * final, because a run's unloaded rows sort after its loaded ones. Ties sort by run, then by position, so equal
     * rows keep their input order.
     *
     * @param list<BucketRun> $runs
     *
     * @return Generator<Rows>
     */
    public function merge(array $runs): Generator
    {
        $order = new RowOrder($this->refs->all());

        /** @var list<BucketCursor> $cursors */
        $cursors = [];
        $schema = null;

        foreach ($runs as $run) {
            $cursor = new BucketCursor($run->rows(), $order, $this->backend);

            if ($cursor->valid()) {
                $schema ??= $cursor->schema();
                $cursors[] = $cursor;
            }
        }

        $schema ??= new Schema();
        $pending = Rows::empty($schema, $this->backend);

        while ($cursors !== []) {
            $frontier = 0;

            foreach ($cursors as $position => $cursor) {
                if (
                    $order->compare(
                        $cursor->keys(),
                        $cursor->batch()->count() - 1,
                        $cursors[$frontier]->keys(),
                        $cursors[$frontier]->batch()->count() - 1,
                    ) < 0
                ) {
                    $frontier = $position;
                }
            }

            $bound = $cursors[$frontier]->keys();
            $boundIndex = $cursors[$frontier]->batch()->count() - 1;
            $slices = [];

            foreach ($cursors as $position => $cursor) {
                $from = $cursor->index();
                $to = $order->firstAfter(
                    $cursor->keys(),
                    $from,
                    $cursor->batch()->count(),
                    $bound,
                    $boundIndex,
                    $position <= $frontier,
                );

                if ($to > $from) {
                    $slices[] = $cursor
                        ->batch()
                        ->slice($from, $to - $from)
                        ->matchTo($schema, $this->backend);
                    $cursor->advance($to);
                }
            }

            $cursors = array_values(array_filter($cursors, static fn(BucketCursor $cursor): bool => $cursor->valid()));
            $block = $slices[0]->concat($this->backend, ...array_slice($slices, 1));
            $pending = $pending->concat(
                $this->backend,
                $block->gather($order->permutation($order->keys($block), $block->count())),
            );

            while ($pending->count() >= $this->batchSize) {
                yield $pending->slice(0, $this->batchSize);
                $pending = $pending->slice($this->batchSize, $pending->count() - $this->batchSize);
            }
        }

        if (!$pending->isEmpty()) {
            yield $pending;
        }
    }
}
