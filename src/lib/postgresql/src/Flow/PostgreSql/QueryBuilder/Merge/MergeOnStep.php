<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Merge;

use Flow\PostgreSql\QueryBuilder\Condition\Condition;

interface MergeOnStep
{
    public function on(Condition $condition): MergeWhenStep;
}
