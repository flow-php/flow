<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\AlterSequence;

use Flow\PostgreSql\Protobuf\AST\AlterObjectSchemaStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface AlterSequenceSchemaFinalStep extends SqlQuery
{
    public function toAst() : AlterObjectSchemaStmt;

    public function toSql() : string;
}
