<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Copy;

use Flow\PgQuery\QueryBuilder\Select\SelectFinalStep;

interface CopyToTableStep
{
    public function query(SelectFinalStep $query) : CopyToDestinationStep;

    public function table(string $table, string ...$columns) : CopyToDestinationStep;
}
