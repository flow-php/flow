<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Role;

use Flow\PostgreSql\Protobuf\AST\DropRoleStmt;

interface DropRoleFinalStep
{
    public function ifExists() : self;

    public function toAst() : DropRoleStmt;

    public function toSql() : string;
}
