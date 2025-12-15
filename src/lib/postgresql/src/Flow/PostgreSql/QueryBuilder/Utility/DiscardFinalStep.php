<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\DiscardStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface DiscardFinalStep extends SqlQuery
{
    public function toAst() : DiscardStmt;

    public function toSql() : string;
}
