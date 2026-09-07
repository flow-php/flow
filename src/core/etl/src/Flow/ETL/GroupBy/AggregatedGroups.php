<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\Row\RowsBuffer;
use Flow\ETL\Rows;
use Generator;

final class AggregatedGroups
{
    /**
     * @var array<string, Group>
     */
    private array $groups = [];

    public function __construct(
        private readonly GroupBy $groupBy,
        private readonly GroupByShape $shape,
    ) {}

    public function accumulate(Rows $batch, FlowContext $context): void
    {
        foreach ($batch as $row) {
            $key = $this->groupBy->keyValues($row, $this->shape->input);
            $group = $this->groups[(string) $key] ??= new Group($key, $this->shape->aggregators->cloned());
            $group->aggregators->aggregate($row, $context);
        }
    }

    /**
     * @param int<1, max> $batchSize
     *
     * @return Generator<Rows>
     */
    public function flush(int $batchSize): Generator
    {
        $buffer = new RowsBuffer($this->shape->output, $batchSize);

        foreach ($this->groups as $group) {
            $aggregated = $this->groupBy->aggregatedRow($group->key, $group->aggregators, $this->shape->output);

            if (null !== ($batch = $buffer->add($aggregated))) {
                yield $batch;
            }
        }

        if (null !== ($batch = $buffer->flush())) {
            yield $batch;
        }
    }
}
