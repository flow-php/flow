<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Session;

use Flow\PgQuery\Protobuf\AST\VariableSetStmt;

interface ResetRoleFinalStep
{
    public function toAst() : VariableSetStmt;
}
