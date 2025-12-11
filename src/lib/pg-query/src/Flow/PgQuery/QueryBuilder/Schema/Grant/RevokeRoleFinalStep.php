<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Grant;

use Flow\PgQuery\Protobuf\AST\GrantRoleStmt;

interface RevokeRoleFinalStep
{
    public function cascade() : self;

    public function restrict() : self;

    public function toAst() : GrantRoleStmt;

    public function toSql() : string;
}
