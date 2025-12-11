<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\AlterView;

use Flow\PostgreSql\Protobuf\AST\AlterObjectSchemaStmt;

interface AlterViewSchemaFinalStep
{
    public function toAst() : AlterObjectSchemaStmt;

    public function toSql() : string;
}
