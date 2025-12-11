<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\AlterMaterializedView;

use Flow\PgQuery\Protobuf\AST\AlterObjectSchemaStmt;

interface AlterMatViewSchemaFinalStep
{
    public function toAst() : AlterObjectSchemaStmt;

    public function toSql() : string;
}
