<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Select;

use Flow\PgQuery\QueryBuilder\Clause\WindowDefinition;

interface SelectWindowStep extends SelectSetOperationStep
{
    public function window(WindowDefinition ...$windows) : SelectSetOperationStep;
}
