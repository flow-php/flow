<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\CreateSequence;

use Flow\PgQuery\Protobuf\AST\CreateSeqStmt;

interface CreateSequenceFinalStep
{
    public function toAst() : CreateSeqStmt;

    public function toSql() : string;
}
