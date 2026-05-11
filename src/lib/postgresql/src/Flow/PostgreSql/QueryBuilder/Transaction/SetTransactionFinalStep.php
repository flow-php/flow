<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Transaction;

use Flow\PostgreSql\Protobuf\AST\VariableSetStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface SetTransactionFinalStep extends Sql
{
    public function toAst(): VariableSetStmt;

    public function toSql(): string;
}
