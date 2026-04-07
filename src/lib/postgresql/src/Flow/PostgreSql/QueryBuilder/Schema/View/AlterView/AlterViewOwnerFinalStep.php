<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\AlterView;

use Flow\PostgreSql\Protobuf\AST\AlterTableStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface AlterViewOwnerFinalStep extends Sql
{
    public function toAst() : AlterTableStmt;

    public function toSql() : string;
}
