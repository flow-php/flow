<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Select;

interface SelectLimitStep extends SelectOffsetStep
{
    public function limit(int $limit) : SelectOffsetStep;
}
