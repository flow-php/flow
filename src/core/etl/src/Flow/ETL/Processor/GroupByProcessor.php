<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\ExternalAggregation;
use Flow\ETL\Processor;
use Generator;

/**
 * Groups all rows and applies aggregation functions.
 *
 * @internal
 */
final readonly class GroupByProcessor implements Processor
{
    public function __construct(
        public GroupBy $groupBy,
    ) {}

    public function process(Generator $rows, FlowContext $context): Generator
    {
        if ($this->groupBy->isPivot()) {
            yield from $this->groupBy->pivotResult($rows, $context);

            return;
        }

        $config = $context->config->grouping;

        yield from (new ExternalAggregation($config->cache, $config->bucketsCount, $config->batchSize))->aggregate(
            $rows,
            $context,
            $this->groupBy,
        );
    }
}
