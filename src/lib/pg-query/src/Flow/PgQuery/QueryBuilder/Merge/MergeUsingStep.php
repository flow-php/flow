<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Merge;

use Flow\PgQuery\QueryBuilder\Select\SelectFinalStep;

interface MergeUsingStep
{
    public function using(string|SelectFinalStep $source, string $alias) : MergeOnStep;
}
