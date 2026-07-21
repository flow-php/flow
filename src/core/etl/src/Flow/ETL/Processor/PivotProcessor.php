<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Generator;

/**
 * @internal
 */
final readonly class PivotProcessor implements Processor
{
    public function __construct(
        private GroupBy $groupBy,
    ) {}

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function process(Generator $rows, FlowContext $context): Generator
    {
        yield from $this->groupBy->pivotResult($rows, $context);
    }
}
