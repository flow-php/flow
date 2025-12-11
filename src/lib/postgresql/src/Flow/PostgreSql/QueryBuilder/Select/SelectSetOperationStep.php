<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

interface SelectSetOperationStep extends SelectOrderByStep
{
    public function except(SelectFinalStep $other) : SelectOrderByStep;

    public function exceptAll(SelectFinalStep $other) : SelectOrderByStep;

    public function intersect(SelectFinalStep $other) : SelectOrderByStep;

    public function intersectAll(SelectFinalStep $other) : SelectOrderByStep;

    public function union(SelectFinalStep $other) : SelectOrderByStep;

    public function unionAll(SelectFinalStep $other) : SelectOrderByStep;
}
