<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
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
    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private GroupBy $groupBy,
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
    public function process(Generator $rows, FlowContext $context): Generator
    {
        yield from $this->groupBy->pivotResult($rows, $context, $this->batchSize);
    }
}
