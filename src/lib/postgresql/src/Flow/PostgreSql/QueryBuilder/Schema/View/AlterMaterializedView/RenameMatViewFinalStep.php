<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\AlterMaterializedView;

use Flow\PostgreSql\Protobuf\AST\RenameStmt;

interface RenameMatViewFinalStep
{
    public function toAst() : RenameStmt;

    public function toSql() : string;
}
