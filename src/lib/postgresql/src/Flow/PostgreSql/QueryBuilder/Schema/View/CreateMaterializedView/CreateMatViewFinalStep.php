<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\CreateMaterializedView;

use Flow\PostgreSql\Protobuf\AST\CreateTableAsStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface CreateMatViewFinalStep extends SqlQuery
{
    public function toAst() : CreateTableAsStmt;

    public function toSql() : string;
}
