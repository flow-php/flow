<?php

declare(strict_types=1);

namespace Flow\ETL\Function\Evaluation;

use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ReferencedColumns;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Rows;

use function count;

final readonly class Selection
{
    /**
     * @param list<int> $indices ascending rows of the batch passed to evaluate()
     */
    public function __construct(
        private array $indices,
    ) {}

    /**
     * $function over the selected rows only; an error names the row of $rows it failed on.
     */
    public function evaluate(ScalarFunction $function, Rows $rows, FlowContext $context): Column
    {
        if (count($this->indices) === $rows->count()) {
            return $function->eval($rows, $context);
        }

        $schema = $rows->schema();
        $names = [];

        // only the columns $function reads are copied; a reference the batch lacks (e.g. inside on_each()) is not one
        foreach ((new ReferencedColumns())->in($function)->all() as $ref) {
            if ($schema->findDefinition($ref->to()) !== null) {
                $names[] = $ref->to();
            }
        }

        $subset = $rows->project($schema->keep(...$names), $context->backend())->gather($this->indices);

        try {
            return $function->eval($subset, $context);
        } catch (EvaluationException $e) {
            throw EvaluationException::at($this->indices[$e->rowIndex], $e);
        }
    }
}
