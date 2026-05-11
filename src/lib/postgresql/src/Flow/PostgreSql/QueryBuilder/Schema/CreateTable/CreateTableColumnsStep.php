<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\CreateTable;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnDefinition;

interface CreateTableColumnsStep extends CreateTableFinalStep
{
    public function column(ColumnDefinition $column): self;
}
