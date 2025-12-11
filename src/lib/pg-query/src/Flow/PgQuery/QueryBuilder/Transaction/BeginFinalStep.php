<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Transaction;

use Flow\PgQuery\Protobuf\AST\TransactionStmt;

interface BeginFinalStep
{
    public function toAst() : TransactionStmt;

    public function toSql() : string;
}
