<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\Column\DefaultBackend;
use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Generator;

use function count;

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
        $pending = [];

        foreach ($this->groups as $group) {
            $pending[] = $this->groupBy->aggregatedValues($group->key, $group->aggregators, $this->shape->output);

            if (count($pending) >= $batchSize) {
                yield (new RowsBuilder($this->shape->output, new DefaultBackend()))
                    ->appendRows($pending)
                    ->finish();
                $pending = [];
            }
        }

        if ($pending !== []) {
            yield (new RowsBuilder($this->shape->output, new DefaultBackend()))
                ->appendRows($pending)
                ->finish();
        }
    }
}
