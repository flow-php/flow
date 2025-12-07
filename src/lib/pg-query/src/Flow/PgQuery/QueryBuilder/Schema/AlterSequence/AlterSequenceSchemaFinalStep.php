<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\AlterSequence;

use Flow\PgQuery\Protobuf\AST\AlterObjectSchemaStmt;

interface AlterSequenceSchemaFinalStep
{
    public function toAst() : AlterObjectSchemaStmt;
}
