<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\CreateSequence;

use Flow\PostgreSql\Protobuf\AST\CreateSeqStmt;

interface CreateSequenceFinalStep
{
    public function toAst() : CreateSeqStmt;

    public function toSql() : string;
}
