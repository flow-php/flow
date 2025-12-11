<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Schema;

use Flow\PostgreSql\Protobuf\AST\RenameStmt;

interface AlterSchemaRenameFinalStep
{
    public function toAst() : RenameStmt;

    public function toSql() : string;
}
