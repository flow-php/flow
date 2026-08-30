<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\Row\RowsBuffer;
use Flow\ETL\Rows;
use Generator;

final readonly class BucketAggregation
{
    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private int $batchSize = 1000,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->batchSize);
        }
    }

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function aggregate(Generator $rows, FlowContext $context, GroupBy $groupBy): Generator
    {
        /** @var array<string, Group> $groups */
        $groups = [];
        $aggregations = $groupBy->aggregations();
        $bound = null;
        $output = null;

        foreach ($rows as $batch) {
            // Bind once per run, against the first non-empty batch's schema - an empty batch has no
            // schema to bind against.
            if ($bound === null) {
                if (!$batch->count()) {
                    continue;
                }

                $schema = $batch->schema();
                $bound = $aggregations->resolved($schema);
                $output = $groupBy->outputSchema($schema, $bound);
            }

            foreach ($batch as $row) {
                $key = $groupBy->keyValues($row);
                $group = $groups[(string) $key] ??= new Group($key, $bound->cloned());
                $group->aggregators->aggregate($row, $context);
            }
        }

        if ($bound === null || $output === null) {
            return;
        }

        $buffer = new RowsBuffer($output, $this->batchSize);

        foreach ($groups as $group) {
            if (null !== ($batch = $buffer->add($groupBy->aggregatedRow($group->key, $group->aggregators, $output)))) {
                yield $batch;
            }
        }

        if (null !== ($batch = $buffer->flush())) {
            yield $batch;
        }
    }
}
