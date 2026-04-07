<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Index\AlterIndex;

use Flow\PostgreSql\Protobuf\AST\RenameStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface RenameIndexFinalStep extends Sql
{
    public function toAst() : RenameStmt;

    public function toSql() : string;
}
