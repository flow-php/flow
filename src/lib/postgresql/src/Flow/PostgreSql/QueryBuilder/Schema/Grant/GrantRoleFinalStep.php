<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Grant;

use Flow\PostgreSql\Protobuf\AST\GrantRoleStmt;

interface GrantRoleFinalStep
{
    public function toAst() : GrantRoleStmt;

    public function toSql() : string;

    public function withAdminOption() : self;
}
