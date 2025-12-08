<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Role;

use Flow\PgQuery\Protobuf\AST\CreateRoleStmt;

interface CreateRoleFinalStep
{
    public function toAst() : CreateRoleStmt;
}
