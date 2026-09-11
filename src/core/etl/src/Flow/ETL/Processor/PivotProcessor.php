<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\PivotAggregation;
use Flow\ETL\GroupBy\PivotShape;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

/**
 * @internal
 */
final class PivotProcessor implements Processor
{
    /**
     * The (input, resolved aggregation, output) triple this step declares. Only bind() sets it; the
     * unbound path derives it from the first batch.
     */
    private ?PivotShape $shape = null;

    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private readonly GroupBy $groupBy,
        private readonly int $batchSize = 1000,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->batchSize);
        }
    }

    /**
     * @throws InvalidArgumentException when a pivot value collides with a group-by column
     * @throws SchemaDefinitionNotFoundException
     */
    public function bind(Schema $input): BoundStep
    {
        $bound = new self($this->groupBy, $this->batchSize);
        $bound->shape = PivotShape::of($this->groupBy, $input);

        return new BoundStep($bound, $bound->shape->output);
    }

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function process(Generator $rows, FlowContext $context): Generator
    {
        $aggregation = new PivotAggregation($this->batchSize);

        yield from $this->shape === null
            ? $aggregation->aggregate($rows, $context, $this->groupBy)
            : $aggregation->aggregateBound($rows, $context, $this->groupBy, $this->shape);
    }
}
