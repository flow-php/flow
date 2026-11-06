<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Extension;

use Flow\PostgreSql\Protobuf\AST\AlterObjectSchemaStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface AlterExtensionSchemaFinalStep extends Sql
{
    public function toAst(): AlterObjectSchemaStmt;

    public function toSql(): string;
}
