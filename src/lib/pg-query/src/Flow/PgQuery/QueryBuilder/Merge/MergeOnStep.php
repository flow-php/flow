<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Merge;

use Flow\PgQuery\QueryBuilder\Condition\Condition;

interface MergeOnStep
{
    public function on(Condition $condition) : MergeWhenStep;
}
