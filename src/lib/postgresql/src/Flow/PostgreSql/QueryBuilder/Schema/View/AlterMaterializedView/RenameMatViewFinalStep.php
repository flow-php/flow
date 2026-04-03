<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\AlterMaterializedView;

use Flow\PostgreSql\Protobuf\AST\RenameStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface RenameMatViewFinalStep extends Sql
{
    public function toAst() : RenameStmt;

    public function toSql() : string;
}
