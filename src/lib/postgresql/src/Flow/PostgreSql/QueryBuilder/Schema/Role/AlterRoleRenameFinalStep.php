<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Role;

use Flow\PostgreSql\Protobuf\AST\RenameStmt;

interface AlterRoleRenameFinalStep
{
    public function toAst() : RenameStmt;

    public function toSql() : string;
}
