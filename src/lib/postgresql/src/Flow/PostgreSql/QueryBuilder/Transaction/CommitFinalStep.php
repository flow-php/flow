<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Transaction;

use Flow\PostgreSql\Protobuf\AST\TransactionStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface CommitFinalStep extends Sql
{
    public function toAst() : TransactionStmt;

    public function toSql() : string;
}
