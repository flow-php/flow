<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\AlterMaterializedView;

use Flow\PostgreSql\Protobuf\AST\AlterObjectSchemaStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface AlterMatViewSchemaFinalStep extends Sql
{
    public function toAst(): AlterObjectSchemaStmt;

    public function toSql(): string;
}
