<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\AlterView;

use Flow\PgQuery\Protobuf\AST\AlterTableStmt;

interface AlterViewOwnerFinalStep
{
    public function toAst() : AlterTableStmt;

    public function toSql() : string;
}
