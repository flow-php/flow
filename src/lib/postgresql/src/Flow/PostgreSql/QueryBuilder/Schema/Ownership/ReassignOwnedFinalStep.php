<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Ownership;

use Flow\PostgreSql\Protobuf\AST\ReassignOwnedStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface ReassignOwnedFinalStep extends SqlQuery
{
    public function toAst() : ReassignOwnedStmt;

    public function toSql() : string;
}
