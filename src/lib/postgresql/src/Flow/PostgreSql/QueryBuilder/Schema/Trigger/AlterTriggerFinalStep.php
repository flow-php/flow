<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Trigger;

use Flow\PostgreSql\Protobuf\AST\AlterObjectDependsStmt;
use Flow\PostgreSql\Protobuf\AST\RenameStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface AlterTriggerFinalStep extends Sql
{
    public function toAst(): RenameStmt|AlterObjectDependsStmt;

    public function toSql(): string;
}
