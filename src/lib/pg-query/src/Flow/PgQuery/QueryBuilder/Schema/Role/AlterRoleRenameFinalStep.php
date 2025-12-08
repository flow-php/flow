<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Role;

use Flow\PgQuery\Protobuf\AST\RenameStmt;

interface AlterRoleRenameFinalStep
{
    public function toAst() : RenameStmt;
}
