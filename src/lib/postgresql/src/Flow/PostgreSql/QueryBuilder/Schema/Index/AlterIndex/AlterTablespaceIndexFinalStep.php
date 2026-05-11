<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Index\AlterIndex;

use Flow\PostgreSql\Protobuf\AST\AlterTableStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface AlterTablespaceIndexFinalStep extends Sql
{
    public function toAst(): AlterTableStmt;

    public function toSql(): string;
}
