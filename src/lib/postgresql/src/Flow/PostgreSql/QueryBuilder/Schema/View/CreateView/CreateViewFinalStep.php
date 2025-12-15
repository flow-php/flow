<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\CreateView;

use Flow\PostgreSql\Protobuf\AST\ViewStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface CreateViewFinalStep extends SqlQuery
{
    public function toAst() : ViewStmt;

    public function toSql() : string;
}
