<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Grant;

use Flow\PgQuery\Protobuf\AST\GrantRoleStmt;

interface GrantRoleFinalStep
{
    public function toAst() : GrantRoleStmt;

    public function toSql() : string;

    public function withAdminOption() : self;
}
