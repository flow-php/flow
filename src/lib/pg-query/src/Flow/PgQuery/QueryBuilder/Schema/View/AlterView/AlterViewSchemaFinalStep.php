<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\AlterView;

use Flow\PgQuery\Protobuf\AST\AlterObjectSchemaStmt;

interface AlterViewSchemaFinalStep
{
    public function toAst() : AlterObjectSchemaStmt;

    public function toSql() : string;
}
