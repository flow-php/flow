<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{condition: Condition\RowCondition, transformer: Transformer}>
 *
 * @psalm-immutable
 */
final class ConditionalTransformer implements Transformer
{
    public function __construct(
        private readonly Transformer\Condition\RowCondition $condition,
        private readonly Transformer $transformer
    ) {
    }

    public function __serialize() : array
    {
        return [
            'condition' => $this->condition,
            'transformer' => $this->transformer,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->condition = $data['condition'];
        $this->transformer = $data['transformer'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        /**
         * @psalm-var pure-callable(Row $row) : array<Row> $transformer
         */
        $transformer = function (Row $row) use ($context) : array {
            if ($this->condition->isMetFor($row)) {
                return (array) $this->transformer->transform(new Rows($row), $context)->getIterator();
            }

            return [$row];
        };

        return $rows->flatMap($transformer);
    }
}
