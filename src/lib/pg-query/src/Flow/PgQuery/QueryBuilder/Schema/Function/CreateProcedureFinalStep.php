<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Function;

use Flow\PgQuery\Protobuf\AST\CreateFunctionStmt;

interface CreateProcedureFinalStep
{
    public function toAst() : CreateFunctionStmt;
}
