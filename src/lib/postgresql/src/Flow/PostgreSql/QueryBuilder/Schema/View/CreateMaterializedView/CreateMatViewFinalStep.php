<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\CreateMaterializedView;

use Flow\PostgreSql\Protobuf\AST\CreateTableAsStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface CreateMatViewFinalStep extends Sql
{
    public function toAst(): CreateTableAsStmt;

    public function toSql(): string;
}
