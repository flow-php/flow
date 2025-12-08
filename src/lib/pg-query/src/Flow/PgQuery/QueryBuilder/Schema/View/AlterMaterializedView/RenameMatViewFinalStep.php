<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\AlterMaterializedView;

use Flow\PgQuery\Protobuf\AST\RenameStmt;

interface RenameMatViewFinalStep
{
    public function toAst() : RenameStmt;
}
