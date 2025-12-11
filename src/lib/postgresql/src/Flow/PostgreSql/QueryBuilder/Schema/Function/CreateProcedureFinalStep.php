<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

use Flow\PostgreSql\Protobuf\AST\CreateFunctionStmt;

interface CreateProcedureFinalStep
{
    public function toAst() : CreateFunctionStmt;

    public function toSql() : string;
}
