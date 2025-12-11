<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\CreateView;

use Flow\PostgreSql\Protobuf\AST\ViewStmt;

interface CreateViewFinalStep
{
    public function toAst() : ViewStmt;

    public function toSql() : string;
}
