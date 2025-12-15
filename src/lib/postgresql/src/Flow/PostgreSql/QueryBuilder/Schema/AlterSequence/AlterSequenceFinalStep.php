<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\AlterSequence;

use Flow\PostgreSql\Protobuf\AST\AlterSeqStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface AlterSequenceFinalStep extends SqlQuery
{
    public function toAst() : AlterSeqStmt;

    public function toSql() : string;
}
