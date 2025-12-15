<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\CreateSequence;

use Flow\PostgreSql\Protobuf\AST\CreateSeqStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface CreateSequenceFinalStep extends SqlQuery
{
    public function toAst() : CreateSeqStmt;

    public function toSql() : string;
}
