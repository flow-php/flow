<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

use Flow\PostgreSql\QueryBuilder\Clause\WindowDefinition;

interface SelectWindowStep extends SelectSetOperationStep
{
    public function window(WindowDefinition ...$windows) : SelectSetOperationStep;
}
