<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\WindowFunction;
use Flow\ETL\Processor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Definition;
use Generator;

use function serialize;

/**
 * Applies window functions over partitioned and ordered data.
 *
 * @internal
 */
final readonly class WindowProcessor implements Processor
{
    /**
     * @param Definition<mixed>|string $entry
     */
    public function __construct(
        private string|Definition $entry,
        private WindowFunction $function,
    ) {}

    public function process(Generator $rows, FlowContext $context): Generator
    {
        $currentPartitionKey = null;
        /** @var array<Row> $partitionRows */
        $partitionRows = [];

        foreach ($rows as $batch) {
            /** @var Rows $batch */
            foreach ($batch as $row) {
                $partitionKey = $this->extractPartitionKey($row);

                if ($currentPartitionKey !== null && $currentPartitionKey !== $partitionKey) {
                    $processedRows = $this->processPartition($partitionRows, $context);

                    if ($processedRows->count() > 0) {
                        yield $processedRows;
                    }

                    $partitionRows = [];
                }

                $partitionRows[] = $row;
                $currentPartitionKey = $partitionKey;
            }
        }

        if ([] !== $partitionRows) {
            $processedRows = $this->processPartition($partitionRows, $context);

            if ($processedRows->count() > 0) {
                yield $processedRows;
            }
        }
    }

    private function extractPartitionKey(Row $row): string
    {
        $partitions = $this->function->window()->partitions();

        if ([] === $partitions) {
            return '__single_partition__';
        }

        $keyParts = [];

        foreach ($partitions as $partition) {
            try {
                $keyParts[] = $row->valueOf($partition);
            } catch (InvalidArgumentException) {
                $keyParts[] = null;
            }
        }

        return serialize($keyParts);
    }

    /**
     * @param array<Row> $rows
     */
    private function processPartition(array $rows, FlowContext $context): Rows
    {
        if ([] === $rows) {
            return new Rows();
        }

        $partitionRows = new Rows(...$rows);

        $orderBy = $this->function->window()->order();

        if ([] !== $orderBy) {
            $partitionRows = $partitionRows->sortBy(...$orderBy);
        }

        $processedRows = [];

        foreach ($partitionRows as $row) {
            $value = $this->function->apply($row, $partitionRows, $context);

            $entryName = $this->entry instanceof Definition ? $this->entry->entry()->name() : $this->entry;

            $newRow = $row->add($context->entryFactory()->create(
                $entryName,
                $value,
                $this->entry instanceof Definition ? $this->entry : null,
            ));

            $processedRows[] = $newRow;
        }

        return new Rows(...$processedRows);
    }
}
