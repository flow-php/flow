<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Session;

use Flow\PostgreSql\Protobuf\AST\VariableSetStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface SetRoleFinalStep extends SqlQuery
{
    public function toAst() : VariableSetStmt;

    public function toSql() : string;
}
