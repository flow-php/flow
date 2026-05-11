<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Ownership;

use Flow\PostgreSql\Protobuf\AST\ReassignOwnedStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface ReassignOwnedFinalStep extends Sql
{
    public function toAst(): ReassignOwnedStmt;

    public function toSql(): string;
}
