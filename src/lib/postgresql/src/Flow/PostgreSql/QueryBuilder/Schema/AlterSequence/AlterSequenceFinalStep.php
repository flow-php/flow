<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\AlterSequence;

use Flow\PostgreSql\Protobuf\AST\AlterSeqStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface AlterSequenceFinalStep extends Sql
{
    public function toAst(): AlterSeqStmt;

    public function toSql(): string;
}
