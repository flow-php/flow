<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Index\AlterIndex;

use Flow\PgQuery\Protobuf\AST\AlterTableStmt;

interface AlterTablespaceIndexFinalStep
{
    public function toAst() : AlterTableStmt;
}
