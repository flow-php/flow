<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Merge;

interface MergeIntoStep
{
    public function into(string $table, ?string $alias = null): MergeUsingStep;
}
