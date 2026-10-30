<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\Evaluation;

use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Functions\ReferencedColumns;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;

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
    public function evaluate(ScalarFunction $function, Rows $rows, Backend $backend): Column
    {
        if (count($this->indices) === $rows->count()) {
            return $function->eval($rows, $backend);
        }

        $schema = $rows->schema();
        $names = [];

        // only the columns $function reads are copied; a reference the batch lacks (e.g. inside on_each()) is not one
        foreach ((new ReferencedColumns())->in($function)->all() as $ref) {
            if ($schema->findDefinition($ref->to()) !== null) {
                $names[] = $ref->to();
            }
        }

        $subset = $rows->project($schema->keep(...$names), $backend)->gather($this->indices);

        try {
            return $function->eval($subset, $backend);
        } catch (EvaluationException $e) {
            throw EvaluationException::at($this->indices[$e->rowIndex], $e);
        }
    }
}
