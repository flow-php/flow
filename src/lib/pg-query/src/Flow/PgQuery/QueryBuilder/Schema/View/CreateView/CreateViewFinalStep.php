<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\CreateView;

use Flow\PgQuery\Protobuf\AST\ViewStmt;

interface CreateViewFinalStep
{
    public function toAst() : ViewStmt;

    public function toSql() : string;
}
