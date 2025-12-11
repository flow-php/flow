<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Index\AlterIndex;

use Flow\PostgreSql\Protobuf\AST\RenameStmt;

interface RenameIndexFinalStep
{
    public function toAst() : RenameStmt;

    public function toSql() : string;
}
