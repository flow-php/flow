<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\AlterMaterializedView;

use Flow\PostgreSql\Protobuf\AST\AlterTableStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface AlterMatViewTablespaceFinalStep extends SqlQuery
{
    public function toAst() : AlterTableStmt;

    public function toSql() : string;
}
