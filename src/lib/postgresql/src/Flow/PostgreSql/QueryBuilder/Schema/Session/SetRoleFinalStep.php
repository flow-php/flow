<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Session;

use Flow\PostgreSql\Protobuf\AST\VariableSetStmt;

interface SetRoleFinalStep
{
    public function toAst() : VariableSetStmt;

    public function toSql() : string;
}
