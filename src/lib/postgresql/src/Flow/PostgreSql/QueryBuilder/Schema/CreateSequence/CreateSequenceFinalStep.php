<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\CreateSequence;

use Flow\PostgreSql\Protobuf\AST\CreateSeqStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface CreateSequenceFinalStep extends Sql
{
    public function toAst() : CreateSeqStmt;

    public function toSql() : string;
}
