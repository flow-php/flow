<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\AlterSequence;

use Flow\PostgreSql\Protobuf\AST\AlterObjectSchemaStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface AlterSequenceSchemaFinalStep extends Sql
{
    public function toAst() : AlterObjectSchemaStmt;

    public function toSql() : string;
}
