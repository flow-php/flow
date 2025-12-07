<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Merge;

interface MergeIntoStep
{
    public function into(string $table, ?string $alias = null) : MergeUsingStep;
}
