<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Transaction;

use Flow\PostgreSql\Protobuf\AST\TransactionStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface SavepointFinalStep extends SqlQuery
{
    public function toAst() : TransactionStmt;

    public function toSql() : string;
}
