<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Role;

use Flow\PostgreSql\Protobuf\AST\DropRoleStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface DropRoleFinalStep extends SqlQuery
{
    public function ifExists() : self;

    public function toAst() : DropRoleStmt;

    public function toSql() : string;
}
