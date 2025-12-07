<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\AlterSequence;

use Flow\PgQuery\Protobuf\AST\AlterTableStmt;

interface AlterSequenceOwnerFinalStep
{
    public function toAst() : AlterTableStmt;
}
