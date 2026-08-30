<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Row;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Partitions;
use Generator;

use function max;

/**
 * Buffers all rows and sorts them in memory. Registered by SortSteps when the sort algorithm is
 * memory_sort().
 *
 * @internal
 */
final readonly class MemorySortProcessor implements Processor
{
    public function __construct(
        private References $refs,
    ) {}

    public function process(Generator $rows, FlowContext $context): Generator
    {
        /** @var array<Row> $buffer */
        $buffer = [];
        $partitions = null;
        $partitionsId = null;
        $maxSize = 1;

        $schema = null;

        foreach ($rows as $batch) {
            $schema ??= $batch->schema();
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

        yield from Rows::partitioned($schema ?? new Schema(), $buffer, $partitions ?? new Partitions())
            ->sortBy(...$this->refs->all())
            ->chunks($maxSize);
    }
}
