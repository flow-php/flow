<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Copy;

interface CopyFromTableStep
{
    public function table(string $table, string ...$columns) : CopyFromSourceStep;
}
