<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Transaction;

use Flow\PgQuery\Protobuf\AST\TransactionStmt;

interface SavepointFinalStep
{
    public function toAst() : TransactionStmt;
}
