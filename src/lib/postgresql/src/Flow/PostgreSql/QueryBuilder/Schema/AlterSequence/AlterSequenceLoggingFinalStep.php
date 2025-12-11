<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\AlterSequence;

use Flow\PostgreSql\Protobuf\AST\AlterTableStmt;

interface AlterSequenceLoggingFinalStep
{
    public function toAst() : AlterTableStmt;

    public function toSql() : string;
}
