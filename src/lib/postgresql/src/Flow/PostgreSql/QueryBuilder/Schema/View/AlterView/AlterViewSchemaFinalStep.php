<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\AlterView;

use Flow\PostgreSql\Protobuf\AST\AlterObjectSchemaStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface AlterViewSchemaFinalStep extends Sql
{
    public function toAst(): AlterObjectSchemaStmt;

    public function toSql(): string;
}
