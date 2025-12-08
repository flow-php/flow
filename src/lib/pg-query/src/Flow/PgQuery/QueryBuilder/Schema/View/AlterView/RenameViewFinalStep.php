<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\AlterView;

use Flow\PgQuery\Protobuf\AST\RenameStmt;

interface RenameViewFinalStep
{
    public function toAst() : RenameStmt;
}
