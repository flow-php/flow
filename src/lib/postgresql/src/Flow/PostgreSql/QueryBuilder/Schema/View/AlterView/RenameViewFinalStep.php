<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\AlterView;

use Flow\PostgreSql\Protobuf\AST\RenameStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface RenameViewFinalStep extends SqlQuery
{
    public function toAst() : RenameStmt;

    public function toSql() : string;
}
