<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\GroupBy;
use Flow\ETL\Rows;
use Generator;

use function count;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

final class PivotedTable
{
    /**
     * @var array<string, array<array-key, AggregatingFunction>> group key => pivot value => accumulator
     */
    private array $accumulators = [];

    /**
     * @var array<string, GroupKey>
     */
    private array $keys = [];

    public function __construct(
        private readonly GroupBy $groupBy,
        private readonly PivotShape $shape,
    ) {}

    public function accumulate(Rows $batch, FlowContext $context): void
    {
        foreach ($batch as $row) {
            $key = $this->groupBy->keyValues($row, $this->shape->input);
            $index = (string) $key;

            $this->keys[$index] ??= $key;
            $this->accumulators[$index] ??= [];

            $pivotValue = $row->get($this->shape->pivot->column);

            if ($pivotValue === null) {
                continue;
            }

            $column = type_union(type_string(), type_integer())->assert($pivotValue);
            $accumulator = $this->accumulators[$index][$column] ??= clone $this->shape->aggregation;
            $accumulator->aggregate($row, $context);
        }
    }

    /**
     * @param int<1, max> $batchSize
     *
     * @throws InvalidArgumentException
     *
     * @return Generator<Rows>
     */
    public function flush(int $batchSize, FlowContext $context): Generator
    {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $batchSize);
        }

        $buffer = [];

        foreach ($this->keys as $index => $key) {
            $row = [];

            /** @var mixed $value */
            foreach ($key as $name => $value) {
                $row[$name] = $value;
            }

            // a combination never seen is null, and a pivot value outside the declared list has no column
            foreach ($this->shape->pivot->values->all() as $column) {
                $row[$column] = ($this->accumulators[$index][$column] ?? null)?->value();
            }

            $buffer[] = $row;

            if (count($buffer) >= $batchSize) {
                yield array_to_rows($buffer, $this->shape->output, $context->hydrator());
                $buffer = [];
            }
        }

        if ($buffer !== []) {
            yield array_to_rows($buffer, $this->shape->output, $context->hydrator());
        }
    }
}
