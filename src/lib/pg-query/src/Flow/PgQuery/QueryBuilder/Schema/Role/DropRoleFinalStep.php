<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Role;

use Flow\PgQuery\Protobuf\AST\DropRoleStmt;

interface DropRoleFinalStep
{
    public function ifExists() : self;

    public function toAst() : DropRoleStmt;
}
