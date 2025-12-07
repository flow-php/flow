<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\AlterSequence;

use Flow\PgQuery\Protobuf\AST\AlterSeqStmt;

interface AlterSequenceFinalStep
{
    public function toAst() : AlterSeqStmt;
}
