<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\AlterView;

use Flow\PostgreSql\Protobuf\AST\AlterTableStmt;

interface AlterViewOwnerFinalStep
{
    public function toAst() : AlterTableStmt;

    public function toSql() : string;
}
