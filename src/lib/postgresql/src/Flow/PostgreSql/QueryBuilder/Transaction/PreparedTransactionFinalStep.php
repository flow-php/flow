<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Transaction;

use Flow\PostgreSql\Protobuf\AST\TransactionStmt;

interface PreparedTransactionFinalStep
{
    public function toAst() : TransactionStmt;

    public function toSql() : string;
}
