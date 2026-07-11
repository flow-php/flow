<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\Filesystem\Partitions;
use Generator;

use function max;

final class MemorySort implements SortingAlgorithm
{
    public function sortGenerator(Generator $rows, FlowContext $context, References $refs): Generator
    {
        /** @var array<Row> $buffer */
        $buffer = [];
        $partitions = null;
        $partitionsId = null;
        $maxSize = 1;

        foreach ($rows as $batch) {
            if ($batch->empty()) {
                continue;
            }

            $maxSize = max($batch->count(), $maxSize);

            if ($partitions === null) {
                $partitions = $batch->partitions();
                $partitionsId = $partitions->id();
            } elseif ($partitionsId !== $batch->partitions()->id()) {
                $partitions = new Partitions();
                $partitionsId = $partitions->id();
            }

            foreach ($batch->all() as $row) {
                $buffer[] = $row;
            }
        }

        yield from Rows::partitioned($buffer, $partitions ?? new Partitions())
            ->sortBy(...$refs->all())
            ->chunks($maxSize);
    }
}
