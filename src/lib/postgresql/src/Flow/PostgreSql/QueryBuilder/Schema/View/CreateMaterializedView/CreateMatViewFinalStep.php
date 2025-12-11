<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\CreateMaterializedView;

use Flow\PostgreSql\Protobuf\AST\CreateTableAsStmt;

interface CreateMatViewFinalStep
{
    public function toAst() : CreateTableAsStmt;

    public function toSql() : string;
}
