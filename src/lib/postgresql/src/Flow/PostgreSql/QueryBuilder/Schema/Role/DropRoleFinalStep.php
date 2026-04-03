<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Role;

use Flow\PostgreSql\Protobuf\AST\DropRoleStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface DropRoleFinalStep extends Sql
{
    public function ifExists() : self;

    public function toAst() : DropRoleStmt;

    public function toSql() : string;
}
