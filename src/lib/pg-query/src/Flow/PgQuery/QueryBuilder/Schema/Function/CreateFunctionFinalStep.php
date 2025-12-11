<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Function;

use Flow\PgQuery\Protobuf\AST\CreateFunctionStmt;

interface CreateFunctionFinalStep
{
    public function toAst() : CreateFunctionStmt;

    public function toSql() : string;
}
