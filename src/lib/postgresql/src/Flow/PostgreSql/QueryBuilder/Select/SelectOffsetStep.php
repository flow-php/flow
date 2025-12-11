<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Select;

interface SelectOffsetStep extends SelectLockingStep
{
    public function offset(int $offset) : SelectLockingStep;
}
