<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Transaction;

use Flow\PostgreSql\Protobuf\AST\VariableSetStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface SetTransactionFinalStep extends SqlQuery
{
    public function toAst() : VariableSetStmt;

    public function toSql() : string;
}
