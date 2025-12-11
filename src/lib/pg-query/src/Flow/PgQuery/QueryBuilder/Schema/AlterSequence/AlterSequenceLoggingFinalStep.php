<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\AlterSequence;

use Flow\PgQuery\Protobuf\AST\AlterTableStmt;

interface AlterSequenceLoggingFinalStep
{
    public function toAst() : AlterTableStmt;

    public function toSql() : string;
}
