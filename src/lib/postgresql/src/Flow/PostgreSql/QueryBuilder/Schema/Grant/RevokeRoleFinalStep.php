<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Grant;

use Flow\PostgreSql\Protobuf\AST\GrantRoleStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface RevokeRoleFinalStep extends SqlQuery
{
    public function cascade() : self;

    public function restrict() : self;

    public function toAst() : GrantRoleStmt;

    public function toSql() : string;
}
