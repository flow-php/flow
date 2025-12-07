<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\CreateTable;

use Flow\PgQuery\QueryBuilder\Schema\ColumnDefinition;

interface CreateTableColumnsStep extends CreateTableFinalStep
{
    public function column(ColumnDefinition $column) : self;
}
