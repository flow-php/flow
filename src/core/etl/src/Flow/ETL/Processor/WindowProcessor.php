<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\FrameAccumulating;
use Flow\ETL\Function\PartitionRanking;
use Flow\ETL\Function\WindowFunction;
use Flow\ETL\Processor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Window\WindowContext;
use Flow\ETL\Window\WindowFrame;
use Generator;

use function array_values;
use function count;
use function Flow\ETL\DSL\rows;
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
                    yield $this->processPartition($partitionRows, $context);

                    $partitionRows = [];
                }

                $partitionRows[] = $row;
                $currentPartitionKey = $partitionKey;
            }
        }

        if ([] !== $partitionRows) {
            yield $this->processPartition($partitionRows, $context);
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
     * Evaluates a frame-only window function for every row of the partition, reusing the previous
     * result whenever the frame bounds did not move.
     *
     * @return array<int, mixed>
     */
    private function accumulateValues(
        FrameAccumulating $function,
        WindowFrame $frame,
        Rows $partition,
        FlowContext $context,
    ): array {
        // Rows guarantees a list internally, but all() is typed array<Row> - narrow it here rather than
        // widen the shared Rows contract. One copy per partition, not per row.
        $rows = array_values($partition->all());
        $count = count($rows);
        $values = [];
        $accumulator = null;
        $previousBounds = null;
        /** @var mixed $value */
        $value = null;

        for ($index = 0; $index < $count; $index++) {
            $bounds = $frame->bounds($index, $partition);

            if ($bounds === $previousBounds) {
                $values[$index] = $value;

                continue;
            }

            // Equal starts with a grown end mean the previous frame is a prefix of this one, so the
            // rows already accumulated still belong to it and only the new tail has to be fed.
            if (
                $accumulator !== null
                && $previousBounds !== null
                && $bounds[0] === $previousBounds[0]
                && $bounds[1] > $previousBounds[1]
            ) {
                for ($i = $previousBounds[1] + 1; $i <= $bounds[1]; $i++) {
                    $accumulator->accumulate($rows[$i]);
                }
            } else {
                $accumulator = $function->accumulator($context);

                for ($i = $bounds[0]; $i <= $bounds[1]; $i++) {
                    $accumulator->accumulate($rows[$i]);
                }
            }

            // @mago-ignore analysis:mixed-assignment
            $value = $accumulator->value();
            $previousBounds = $bounds;
            $values[$index] = $value;
        }

        return $values;
    }

    /**
     * Both call sites guarantee a non-empty partition - one is guarded by `[] !== $partitionRows`, the
     * other by a non-null current partition key, which is only set after a row has been appended.
     *
     * @param array<Row> $rows
     */
    private function processPartition(array $rows, FlowContext $context): Rows
    {
        $window = $this->function->window();
        $orderBy = $window->order();
        $partitionRows = rows(...$rows)->sortBy(...$orderBy ?: $window->partitions());

        $frame = $window->frame();
        $processedRows = [];
        $entryName = $this->entry instanceof Definition ? $this->entry->entry()->name() : $this->entry;

        $values = match (true) {
            $this->function instanceof PartitionRanking => $this->function->rankPartition($partitionRows),
            $this->function instanceof FrameAccumulating => $this->accumulateValues(
                $this->function,
                $frame,
                $partitionRows,
                $context,
            ),
            default => null,
        };

        foreach ($partitionRows as $index => $row) {
            // @mago-ignore analysis:mixed-assignment
            $value = $values === null
                ? $this->function->apply(new WindowContext($row, $index, $partitionRows, $frame, $context))
                : $values[$index];

            $newRow = $row->add(
                $this->entry instanceof Definition
                    ? $context->entryFactory()->create(
                        $entryName,
                        $value,
                        $this->entry->type(),
                        $this->entry->metadata(),
                    )
                    : $context->entryFactory()->create($entryName, $value),
            );

            $processedRows[] = $newRow;
        }

        return rows(...$processedRows);
    }
}
