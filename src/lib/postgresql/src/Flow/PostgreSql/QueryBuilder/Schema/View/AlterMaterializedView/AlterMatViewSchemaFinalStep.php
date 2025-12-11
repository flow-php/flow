<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\AlterMaterializedView;

use Flow\PostgreSql\Protobuf\AST\AlterObjectSchemaStmt;

interface AlterMatViewSchemaFinalStep
{
    public function toAst() : AlterObjectSchemaStmt;

    public function toSql() : string;
}
