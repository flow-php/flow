<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\Pivot;
use Flow\ETL\GroupBy\PivotSchema;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Processor;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
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
        private Pivot $pivot,
        private int $batchSize = 1000,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->batchSize);
        }
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, (new PivotSchema())->of(
            $input,
            References::init(...$this->groupBy->references()),
            $this->pivot->values->all(),
            $this->groupBy->aggregations()->first(),
        ));
    }

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function process(Generator $rows, FlowContext $context): Generator
    {
        yield from $this->groupBy->pivotResult($rows, $context, $this->pivot, $this->batchSize);
    }
}
