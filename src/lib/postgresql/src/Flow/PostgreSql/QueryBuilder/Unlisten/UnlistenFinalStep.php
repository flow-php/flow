<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Unlisten;

use Flow\PostgreSql\Protobuf\AST\UnlistenStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface UnlistenFinalStep extends Sql
{
    public function toAst() : UnlistenStmt;
}
