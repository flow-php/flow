<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Grant;

use Flow\PostgreSql\Protobuf\AST\GrantRoleStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface GrantRoleFinalStep extends SqlQuery
{
    public function toAst() : GrantRoleStmt;

    public function toSql() : string;

    public function withAdminOption() : self;
}
