<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
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
        foreach ($rows as $batch) {
            /** @var Rows $batch */
            $this->groupBy->group($batch, $context);
        }

        yield $this->groupBy->result($context);
    }
}
