<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Grant;

use Flow\PostgreSql\Protobuf\AST\GrantRoleStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface GrantRoleFinalStep extends Sql
{
    public function toAst(): GrantRoleStmt;

    public function toSql(): string;

    public function withAdminOption(): self;
}
