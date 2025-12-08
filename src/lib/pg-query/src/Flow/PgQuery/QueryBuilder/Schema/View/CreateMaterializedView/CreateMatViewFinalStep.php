<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\CreateMaterializedView;

use Flow\PgQuery\Protobuf\AST\CreateTableAsStmt;

interface CreateMatViewFinalStep
{
    public function toAst() : CreateTableAsStmt;
}
