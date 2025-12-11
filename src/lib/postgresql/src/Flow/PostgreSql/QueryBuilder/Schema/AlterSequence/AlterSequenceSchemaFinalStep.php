<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\AlterSequence;

use Flow\PostgreSql\Protobuf\AST\AlterObjectSchemaStmt;

interface AlterSequenceSchemaFinalStep
{
    public function toAst() : AlterObjectSchemaStmt;

    public function toSql() : string;
}
