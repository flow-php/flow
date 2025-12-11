<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Transaction;

use Flow\PostgreSql\Protobuf\AST\TransactionStmt;

interface CommitFinalStep
{
    public function toAst() : TransactionStmt;

    public function toSql() : string;
}
