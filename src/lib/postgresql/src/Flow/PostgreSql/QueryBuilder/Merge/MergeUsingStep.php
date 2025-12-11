<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Merge;

use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;

interface MergeUsingStep
{
    public function using(string|SelectFinalStep $source, string $alias) : MergeOnStep;
}
