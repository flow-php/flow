<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Sort\Merge;

use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Sort\RowOrder;
use Flow\ETL\Values\Sort\SortColumn;
use Generator;

use function array_filter;
use function array_values;

final readonly class KWayMerge
{
    /**
     * @param list<SortColumn> $columns
     */
    public function __construct(
        private array $columns,
        private Backend $backend,
        private BatchSize $batchSize = new BatchSize(1000),
    ) {}

    /**
     * Merges block by block: every loaded row that sorts at or before the smallest last loaded row (the frontier) is
     * final, because a stream's unloaded rows sort after its loaded ones. Ties sort by stream, then by position, so
     * equal rows keep their input order.
     *
     * @param list<Generator<Rows>> $streams each sorted in this order
     *
     * @return Generator<Rows>
     */
    public function merge(array $streams): Generator
    {
        $order = new RowOrder($this->columns);

        /** @var list<BucketCursor> $cursors */
        $cursors = [];
        $schema = null;

        foreach ($streams as $stream) {
            $cursor = new BucketCursor($stream, $order, $this->backend);

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
            $block = Rows::concat($slices[0]->schema(), $this->backend, ...$slices);
            $pending = Rows::concat(
                $pending->schema(),
                $this->backend,
                $pending,
                $this->backend->sort($block, $this->columns),
            );

            while ($pending->count() >= $this->batchSize->size) {
                yield $pending->slice(0, $this->batchSize->size);
                $pending = $pending->slice($this->batchSize->size, $pending->count() - $this->batchSize->size);
            }
        }

        if (!$pending->isEmpty()) {
            yield $pending;
        }
    }
}
