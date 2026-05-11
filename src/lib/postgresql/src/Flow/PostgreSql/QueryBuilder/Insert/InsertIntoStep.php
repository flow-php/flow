<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Insert;

use Flow\PostgreSql\QueryBuilder\Table\Table;

interface InsertIntoStep
{
    public function into(string|Table $table, ?string $alias = null): InsertColumnsStep;
}
