<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

use Flow\PostgreSql\Protobuf\AST\CreateFunctionStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface CreateFunctionFinalStep extends SqlQuery
{
    public function toAst() : CreateFunctionStmt;

    public function toSql() : string;
}
