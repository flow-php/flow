<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Trigger;

use Flow\PgQuery\Protobuf\AST\{AlterObjectDependsStmt, RenameStmt};

interface AlterTriggerFinalStep
{
    public function toDependsAst() : AlterObjectDependsStmt;

    public function toRenameAst() : RenameStmt;
}
