<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Trigger;

use Flow\PgQuery\Protobuf\AST\{AlterObjectDependsStmt, RenameStmt};

interface AlterTriggerFinalStep
{
    public function toAst() : RenameStmt|AlterObjectDependsStmt;

    public function toSql() : string;
}
