<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Copy;

use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;

interface CopyToTableStep
{
    public function query(SelectFinalStep $query): CopyToDestinationStep;

    public function table(string $table, string ...$columns): CopyToDestinationStep;
}
