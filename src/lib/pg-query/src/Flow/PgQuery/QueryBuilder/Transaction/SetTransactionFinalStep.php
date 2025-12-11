<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Transaction;

use Flow\PgQuery\Protobuf\AST\VariableSetStmt;

interface SetTransactionFinalStep
{
    public function toAst() : VariableSetStmt;

    public function toSql() : string;
}
