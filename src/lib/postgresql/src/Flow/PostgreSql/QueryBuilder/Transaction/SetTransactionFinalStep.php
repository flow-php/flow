<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Transaction;

use Flow\PostgreSql\Protobuf\AST\VariableSetStmt;

interface SetTransactionFinalStep
{
    public function toAst() : VariableSetStmt;

    public function toSql() : string;
}
