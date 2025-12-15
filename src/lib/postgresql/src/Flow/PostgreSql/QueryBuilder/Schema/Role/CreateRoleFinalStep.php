<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Role;

use Flow\PostgreSql\Protobuf\AST\CreateRoleStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface CreateRoleFinalStep extends SqlQuery
{
    public function toAst() : CreateRoleStmt;

    public function toSql() : string;
}
