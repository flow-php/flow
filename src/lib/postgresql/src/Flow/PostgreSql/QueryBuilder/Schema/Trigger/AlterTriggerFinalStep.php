<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Trigger;

use Flow\PostgreSql\Protobuf\AST\{AlterObjectDependsStmt, RenameStmt};
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface AlterTriggerFinalStep extends SqlQuery
{
    public function toAst() : RenameStmt|AlterObjectDependsStmt;

    public function toSql() : string;
}
