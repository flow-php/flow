<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Trigger;

use Flow\PostgreSql\Protobuf\AST\{AlterObjectDependsStmt, RenameStmt};

interface AlterTriggerFinalStep
{
    public function toAst() : RenameStmt|AlterObjectDependsStmt;

    public function toSql() : string;
}
