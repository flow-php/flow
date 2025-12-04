<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Insert;

interface InsertIntoStep
{
    public function into(string $table, ?string $alias = null) : InsertColumnsStep;
}
