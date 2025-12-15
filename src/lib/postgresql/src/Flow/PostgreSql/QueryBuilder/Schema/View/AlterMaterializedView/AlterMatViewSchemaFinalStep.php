<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\AlterMaterializedView;

use Flow\PostgreSql\Protobuf\AST\AlterObjectSchemaStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface AlterMatViewSchemaFinalStep extends SqlQuery
{
    public function toAst() : AlterObjectSchemaStmt;

    public function toSql() : string;
}
