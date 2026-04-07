<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\CreateView;

use Flow\PostgreSql\Protobuf\AST\ViewStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface CreateViewFinalStep extends Sql
{
    public function toAst() : ViewStmt;

    public function toSql() : string;
}
