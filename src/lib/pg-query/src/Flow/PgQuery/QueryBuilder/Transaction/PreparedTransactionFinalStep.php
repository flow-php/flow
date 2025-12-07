<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Transaction;

use Flow\PgQuery\Protobuf\AST\TransactionStmt;

interface PreparedTransactionFinalStep
{
    public function toAst() : TransactionStmt;
}
