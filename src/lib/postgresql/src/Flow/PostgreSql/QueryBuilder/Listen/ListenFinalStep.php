<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Listen;

use Flow\PostgreSql\Protobuf\AST\ListenStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface ListenFinalStep extends Sql
{
    public function toAst() : ListenStmt;
}
