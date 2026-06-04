<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

interface SelectSetOperationStep extends SelectOrderByStep
{
    public function except(SelectFinalStep $other): SelectSetOperationStep;

    public function exceptAll(SelectFinalStep $other): SelectSetOperationStep;

    public function intersect(SelectFinalStep $other): SelectSetOperationStep;

    public function intersectAll(SelectFinalStep $other): SelectSetOperationStep;

    public function union(SelectFinalStep $other): SelectSetOperationStep;

    public function unionAll(SelectFinalStep $other): SelectSetOperationStep;
}
